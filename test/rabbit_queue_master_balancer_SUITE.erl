%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Queue Master Balancer.
%%
%% The Developer of this component is Erlang Solutions, Ltd.
%% Copyright (c) 2017-2018 Erlang Solutions, Ltd. All rights reserved.
%%

-module(rabbit_queue_master_balancer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_queue_master_balancer.hrl").

-compile(export_all).
-import(rabbit_misc, [pget/2, pget/3]).

-define(VHOST,   <<"/">>).
-define(EXHANGE, <<"qbx">>).
-define(RK,      <<"qbr">>).
-define(TEST_OPERATIONAL_PRIORITY,        15).
-define(TEST_PRELOAD_QUEUES,              false).
-define(TEST_QUEUE_EQUILIBRIUM,           ignore).
-define(TEST_SYNC_DELAY_TIMEOUT,          15000).
-define(TEST_SYNC_VERIFICATION_FACTOR,    100).
-define(TEST_MASTER_VERIFICATION_TIMEOUT, 20000).
-define(TEST_POLICY_TRANSITION_DELAY,     100).

all() ->
    [
      {group, non_parallel_state_tests}
    ].

groups() ->
    [
      {non_parallel_state_tests, [], [
          successful_queue_balancing_configuration,
          successful_queue_balancing_no_ha_no_messages,
          successful_queue_balancing_with_ha_no_messages,
          successful_queue_balancing_with_ha_and_messages,
          successful_queue_balancing_with_ha_75_queues_300_messages_notraffic,
          successful_queue_balancing_with_ha_75_queues_300_messages_traffic,
          unsuccessful_queue_balancing_no_ha_with_messages,

          successful_queue_high_equilibrium_with_ha_and_messages,
          successful_queue_low_equilibrium_with_ha_and_messages,
          successful_queue_100_percent_equilibrium_with_ha_and_messages,
          successful_queue_unsupported_high_equilibrium_with_ha_and_messages,
          successful_queue_unsupported_low_equilibrium_with_ha_and_messages,
          successful_queue_already_balanced_equilibrium_with_ha_and_messages
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(non_parallel_state_tests, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_count, 3}
      ]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ maybe_ha(Testcase)),
    ok = rabbit_ct_broker_helpers:rpc(Config2, 0,
      application, stop, [rabbitmq_queue_master_balancer]),
    Config2.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% ----------------------------------
%% Test Cases I - without equilibrium
%% ----------------------------------
successful_queue_balancing_configuration(Config) ->
  [A|_]   = _Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Info = info(Config, A),
  true = (pget(operational_priority, Info) == ?TEST_OPERATIONAL_PRIORITY),
  true = (pget(preload_queues, Info) == ?TEST_PRELOAD_QUEUES),
  true = (pget(queue_equilibrium, Info) == ?TEST_QUEUE_EQUILIBRIUM),
  true = (pget(sync_delay_timeout, Info) == ?TEST_SYNC_DELAY_TIMEOUT),
  true = (pget(sync_verification_factor, Info) == ?TEST_SYNC_VERIFICATION_FACTOR),
  true = (pget(master_verification_timeout, Info) == ?TEST_MASTER_VERIFICATION_TIMEOUT),
  true = (pget(policy_transition_delay, Info) == ?TEST_POLICY_TRANSITION_DELAY),
  passed.

successful_queue_balancing_no_ha_no_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 0,
  Delay   = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_run(Config, Queues, Nodes, Delay).

successful_queue_balancing_with_ha_no_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 0,
  Delay   = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed  = successful_run(Config, Queues, Nodes, Delay),
  passed  = verify_ha(Config).

successful_queue_balancing_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues   = 4,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_run(Config, Queues, Nodes, Delay),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_balancing_with_ha_75_queues_300_messages_notraffic(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues   = 75,
  Messages = 300,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_run(Config, Queues, Nodes, Delay),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_balancing_with_ha_75_queues_300_messages_traffic(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues   = 75,
  Messages = 200,
  ActiveMessages = 100,
  Rate  = 10, %% Messages per second
  Delay = 50,
  {ok, Ch} = init_queues(Config, 0, Queues, Messages),
  generate_traffic(Ch, ActiveMessages, Rate),
  passed = successful_run(Config, Queues, Nodes, Delay),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages + ActiveMessages).

unsuccessful_queue_balancing_no_ha_with_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 10,
  Delay   = 50,
  init_queues(Config, 0, Queues, Messages),
  passed = unsuccessful_run(Config, Queues, Nodes, Delay),
  passed = verify_messages(Config, 0, Messages).

%% --------------------------------
%% Test Cases II - with equilibrium
%% --------------------------------
successful_queue_high_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A, QEQ = 80),
  Queues   = 20,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, QEQ),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_low_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A, QEQ = 55),
  Queues   = 20,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, QEQ),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_100_percent_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A, QEQ = 100),
  Queues   = 20,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, QEQ),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_unsupported_high_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A, 110),
  Queues   = 20,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, ?MIN_QEQ),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_unsupported_low_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A, 25),
  Queues   = 20,
  Messages = 10,
  Delay    = 50,
  {ok, _Ch} = init_queues(Config, 0, Queues, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, ?MIN_QEQ),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

successful_queue_already_balanced_equilibrium_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  QArgs  = [{<<"x-queue-master-locator">>, longstr, <<"min-masters">>}],
  setup_queue_master_balancer(Config, A, QEQ = 60),
  Queues   = 20,
  Messages = 10,
  Delay    = 0, %% low delay - queues already balanced by <<"min-masters">>
  {ok, _Ch} = init_queues(Config, 0, Queues, QArgs, Messages),
  passed = successful_equilibrium_run(Config, Queues, Nodes, Delay, QEQ, QArgs),
  passed = verify_ha(Config),
  passed = verify_messages(Config, 0, Messages).

%% --------
%% Internal
%% --------
successful_run(Config, N, [A, B, C], Delay) ->
  QNodes0 = get_queue_nodes(Config),
  true = (occurance(A, QNodes0) == N),
  true = (occurance(B, QNodes0) == 0),
  true = (occurance(C, QNodes0) == 0),

  %% Idle: Info0
  Info0 = info(Config, A),
  Status0 = status(Config, A),
  ?STATE_IDLE = pget(phase, Info0),
  QNodes = [FN|_] = get_queue_nodes(Config),
  NNodes = length(QNodes),
  NNodes = occurance(FN, QNodes, 0),

  %% Load Queues: Info1
  {ok, N} = load_queues(Config, A),
  Info1   = info(Config, A),
  Status1 = status(Config, A),

  %% Balance Queues: Info2
  ok    = balance_queues(Config, A),
  Info2 = info(Config, A),
  Status2 = status(Config, A),

  %% ensure intermediate operations during balancing don't
  %% cause crash, i.e. loading queues during balance phase
  {error, {not_allowed, {'$load_queues', ?STATE_BALANCING_QUEUES}}}
    = load_queues(Config, A),

  %% Balance Pause: Info3
  ok    = pause(Config, A),
  Info3 = info(Config, A),

  %% Balance Continue: Info4
  ok    = continue(Config, A),
  Info4 = info(Config, A),

  %% Wait for Balancing Complete: Info5
  {ok, Info5} = wait_until_complete(Config, A, Delay),

  %% Stop: Info6
  ok    = stop(Config, A),
  Info6 = info(Config, A),

  %% Reset: Info7
  ok    = reset(Config, A),
  Info7 = info(Config, A),
  Status7 = status(Config, A),

  %% Verify Idle Info0
  ?STATE_IDLE = pget(phase, Info0),
  true  = (pget(total,    Info0) == 0),
  true  = (pget(position, Info0) == undefined),
  false = (pget(balanced, Info0) >  0),

  %% Verify Load Queues Info1
  ?STATE_READY = pget(phase, Info1),
  true = (pget(total,    Info1) == N),
  true = (pget(position, Info1) >= 0),
  false= (pget(balanced, Info1) >  0),

  %% Verify Info2
  ?STATE_BALANCING_QUEUES = pget(phase, Info2),
  true = (pget(total,    Info2) == N),
  true = (pget(position, Info2) >= 0),
  true = (pget(balanced, Info2) >  0),

  %% Verify Info3
  ?STATE_PAUSE = pget(phase, Info3),
  true = (pget(total,    Info3) == N),
  true = (pget(position, Info3) >= 0),
  true = (pget(balanced, Info3) >  0),

  %% Verify Info4
  ?STATE_BALANCING_QUEUES = pget(phase, Info4),
  true = (pget(total,    Info4) == N),
  true = (pget(position, Info4) >= 0),
  true = (pget(balanced, Info4) >  0),

  %% Verify Balancing Complete: Info5
  ?STATE_IDLE = pget(phase, Info5),
  true = (pget(total,    Info5) == N),
  true = (pget(position, Info5) >= 0),
  true = (pget(balanced, Info5) == N),

  %% Verify Stop: Info6
  ?STATE_IDLE = pget(phase, Info6),
  true = (pget(total,    Info6) == N),
  true = (pget(position, Info6) == undefined),
  true = (pget(balanced, Info6) == N),

  %% Verify Reset: Info7
  ?STATE_IDLE = pget(phase, Info7),
  true = (pget(total,    Info7) == N),
  true = (pget(position, Info7) == undefined),
  true = (pget(balanced, Info7) == 0),

  true = (ets:info(rabbit_queue_master_balancer, size) == undefined),

  %% Verify Status
  Pid  = pget(process_id, Status0),
  true = is_pid_alive(Config, Pid),
  true = (pget(process_id, Status1) == Pid),
  true = (pget(process_id, Status2) == Pid),
  true = (pget(process_id, Status7) == Pid),
  true = (pget(queues_pending_balance, Status0) == 0),
  true = (pget(queues_pending_balance, Status1) == N),
  true = (pget(queues_pending_balance, Status2) >  0),
  true = (pget(queues_pending_balance, Status7) == 0),
  true = (pget(memory_utilization,     Status7) >  0),

  %% We Qualify Queue Equilibrium as follows;
  %% => at least 2 queues per node passes from
  %%    a total of 7 qualifies the cluster as
  %%    having attained balance/equilibrium...
  {ok, Report} = report(Config, A),

  {queues, AQs} = pget(A, Report),
  {queues, BQs} = pget(B, Report),
  {queues, CQs} = pget(C, Report),
  true = (AQs >= 1),
  true = (BQs >= 1),
  true = (CQs >= 1),

  passed.

unsuccessful_run(Config, N, [A, B, C], Delay) ->
  QNodes0 = get_queue_nodes(Config),
  true = (occurance(A, QNodes0) == N),
  true = (occurance(B, QNodes0) == 0),
  true = (occurance(C, QNodes0) == 0),

  %% Idle: Info0
  Info0 = info(Config, A),
  ?STATE_IDLE = pget(phase, Info0),

  QNodes = [FN|_] = get_queue_nodes(Config),
  NNodes = length(QNodes),
  NNodes = occurance(FN, QNodes, 0),

  %% Load Queues: Info1
  {ok, N} = load_queues(Config, A),
  ok    = balance_queues(Config, A),
  ok    = pause(Config, A),
  ok    = continue(Config, A),
  delay(Delay),
  ok    = stop(Config, A),
  ok    = reset(Config, A),

  %% Unsuccessful run, queues remain unbalanced,
  %% all residing on node 'A'.
  {ok, Report} = report(Config, A),
  {queues, AQs} = pget(A, Report),
  {queues, BQs} = pget(B, Report),
  {queues, CQs} = pget(C, Report),
  true = (AQs == N),
  true = (BQs == 0),
  true = (CQs == 0),

  passed.


successful_equilibrium_run(Config, N, [A, B, C], Delay, QEQ) ->
  successful_equilibrium_run(Config, N, [A, B, C], Delay, QEQ, []).

successful_equilibrium_run(Config, N, [A, B, C], Delay, QEQ, QArgs) ->
  QNodes0 = get_queue_nodes(Config),
  AvgQs   = N div 3,
  ?MIN_MASTERS_FILTER(QArgs,
    fun() ->
      true = (occurance(A, QNodes0) >= AvgQs),
      true = (occurance(B, QNodes0) >= AvgQs),
      true = (occurance(C, QNodes0) >= AvgQs)
    end,
    fun() ->
      true = (occurance(A, QNodes0) == N),
      true = (occurance(B, QNodes0) == 0),
      true = (occurance(C, QNodes0) == 0)
    end),

  %% Idle: Info0
  Info0 = info(Config, A),
  Status0 = status(Config, A),
  ?STATE_IDLE = pget(phase, Info0),
  QNodes = [FN|_] = get_queue_nodes(Config),
  NNodes = length(QNodes),
  true = ?MIN_MASTERS_FILTER(QArgs,
            fun() -> (NNodes div 3) >= AvgQs end,
            fun() -> NNodes =:= occurance(FN, QNodes, 0) end),

  %% Load Queues: Info1
  {ok, N} = load_queues(Config, A),
  Info1   = info(Config, A),
  Status1 = status(Config, A),

  %% Balance Queues: Info2
  ok    = balance_queues(Config, A),
  Info2 = info(Config, A),
  Status2 = status(Config, A),

  %% Wait for Balancing Complete: Info3
  {ok, Info3} = wait_until_complete(Config, A, Delay),

  %% Stop: Info4
  ok    = stop(Config, A),
  Info4 = info(Config, A),

  %% Reset: Info5
  ok    = reset(Config, A),
  Info5 = info(Config, A),
  Status5 = status(Config, A),

  %% Verify Idle Info0
  ?STATE_IDLE = pget(phase, Info0),
  true  = (pget(total,    Info0) == 0),
  true  = (pget(position, Info0) == undefined),
  false = (pget(balanced, Info0) >  0),

  %% Verify Load Queues Info1
  ?STATE_READY = pget(phase, Info1),
  true = (pget(total,    Info1) == N),
  true = (pget(position, Info1) >= 0),
  false= (pget(balanced, Info1) >  0),

  %% Verify Info2
  ?STATE_BALANCING_QUEUES = pget(phase, Info2),
  true = (pget(total,    Info2) == N),
  true = (pget(position, Info2) >= 0),
  true = (pget(balanced, Info2) >  0),

  %% Verify Balancing Complete: Info3
  Equilibria = N / 3,  %% equilibrium
  Threshold  = floor_value((QEQ / 100) * Equilibria), %% effective threshold
  Balanced   = calc_balanced(Threshold, QArgs), %% the minimum number of ((Threshold * Nodes-1)

  ?STATE_IDLE = pget(phase, Info3),
  true = (pget(total,    Info3) == N),
  true = (pget(position, Info3) >= 0),
  true = (pget(balanced, Info3) >= Balanced),

  %% Verify Stop: Info4
  ?STATE_IDLE = pget(phase, Info4),
  true = (pget(total,    Info4) == N),
  true = (pget(position, Info4) == undefined),
  true = (pget(balanced, Info4) >= Balanced),

  %% Verify Reset: Info5
  ?STATE_IDLE = pget(phase, Info5),
  true = (pget(total,    Info5) == N),
  true = (pget(position, Info5) == undefined),
  true = (pget(balanced, Info5) == 0),

  true = (ets:info(rabbit_queue_master_balancer, size) == undefined),

  %% Verify Status
  Pid  = pget(process_id, Status0),
  true = is_pid_alive(Config, Pid),
  true = (pget(process_id, Status1) == Pid),
  true = (pget(process_id, Status2) == Pid),
  true = (pget(process_id, Status5) == Pid),
  true = (pget(queues_pending_balance, Status0) == 0),
  true = (pget(queues_pending_balance, Status1) == N),
  true = ?MIN_MASTERS_FILTER(QArgs,
            fun() -> pget(queues_pending_balance, Status2) =:=  0 end,
            fun() -> pget(queues_pending_balance, Status2) >    0 end),
  true = (pget(queues_pending_balance, Status5) == 0),
  true = (pget(memory_utilization,     Status5) >  0),

  %% We Qualify Queue Equilibrium as follows;
  %% => NumQueues >= QEQ% of Total
  {ok, Report} = report(Config, A),

  {queues, AQs} = pget(A, Report),
  {queues, BQs} = pget(B, Report),
  {queues, CQs} = pget(C, Report),

  true = (AQs >= Threshold),
  true = (BQs >= Threshold),
  true = (CQs >= Threshold),

  passed.

%% ---------
%% Internal
%% ---------
init_queues(Config, Node, NQs, NMsgs) ->
  init_queues(Config, Node, NQs, [], NMsgs).

init_queues(Config, Node, NQs, QArgs, NMsgs) ->
  Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
  setup_exchange(Ch),
  declare_queues(Ch, QArgs, NQs),
  publish(Ch, NMsgs),
  {ok, Ch}.

setup_exchange(Ch) ->
    XD =  #'exchange.declare'{exchange = ?EXHANGE,
                              type = <<"fanout">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, XD).

declare_queues(Ch, QArgs, NQs) ->
 [begin
    QN = list_to_binary("test.q."++integer_to_list(N)),
    QD = #'queue.declare'{queue = QN, arguments = QArgs},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, QD),

    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{queue       = QN,
                                            exchange    = ?EXHANGE,
                                            routing_key = ?RK})
  end || N <- lists:seq(1, NQs)],
  ok.

%% Very simple traffic generator. Spawn a publisher at passed Rate
%% every second till messages aggregate have been published
generate_traffic(_Ch, 0, _Rate) -> ok;
generate_traffic(Ch, Aggregate, Rate) when Aggregate < Rate ->
    %% Send remaining messages
    _Pid = spawn(fun() -> publish(Ch, Aggregate) end);
generate_traffic(Ch, Aggregate, Rate) when Aggregate >= Rate ->
    _Pid = spawn(fun() -> publish(Ch, Rate) end),
    timer:sleep(1000),
    generate_traffic(Ch, Aggregate - Rate, Rate).

publish(Ch, NMsgs) ->
  [begin
     Msg = list_to_binary("test.message."++integer_to_list(N)),
     Publish = #'basic.publish'{exchange    = ?EXHANGE,
                                routing_key = ?RK},
     amqp_channel:call(Ch, Publish, #amqp_msg{payload = Msg})
   end || N <- lists:seq(1, NMsgs)].

maybe_ha(successful_queue_balancing_no_ha_no_messages) -> no_ha();
maybe_ha(unsuccessful_queue_balancing_no_ha_with_messages) -> no_ha();
maybe_ha(_Any) -> ha().

ha()    -> [fun rabbit_ct_broker_helpers:set_ha_policy_all/1].
no_ha() -> [].

get_queue_nodes(Config) ->
  rabbit_ct_broker_helpers:rpc(Config, 0,
    ?MODULE, get_queue_nodes1, []).

get_queue_nodes1() ->
  [node(case Q of
          {amqqueue, {resource,?VHOST,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live} ->
            Pid;
          {amqqueue, {resource,?VHOST,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live,_} ->
            Pid;
          {amqqueue, {resource,?VHOST,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,_,live,_,_,_,_} ->
            Pid
        end) || Q <- rabbit_amqqueue:list()].

get_queue_names_and_nodes(Config) ->
  [begin
    {QN0, Pid0} =
      case Q of
        {amqqueue, {resource,?VHOST,queue,QN},_,_,_,_,Pid,_,_,_,_,_,_,live} ->
          {QN, Pid};
        {amqqueue, {resource,?VHOST,queue,QN},_,_,_,_,Pid,_,_,_,_,_,_,live,_} ->
          {QN, Pid};
        {amqqueue, {resource,?VHOST,queue,QN},_,_,_,_,Pid,_,_,_,_,_,_,_,live,_,_,_,_} ->
          {QN, Pid}
      end,
    {QN0, node(Pid0)}
   end || Q <- rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, [])].

setup_queue_master_balancer(Config, Node) ->
  setup_queue_master_balancer(Config, Node, ?TEST_QUEUE_EQUILIBRIUM).

setup_queue_master_balancer(Config, Node, Equilibrium) ->
  ok = rabbit_ct_broker_helpers:rpc(Config, Node,
         ?MODULE, init_queue_master_balancer_remote, [Equilibrium]).

init_queue_master_balancer_remote(Equilibrium) ->
  application:set_env(rabbitmq_queue_master_balancer, operational_priority, ?TEST_OPERATIONAL_PRIORITY),
  application:set_env(rabbitmq_queue_master_balancer, preload_queues, ?TEST_PRELOAD_QUEUES),
  application:set_env(rabbitmq_queue_master_balancer, queue_equilibrium, Equilibrium),
  application:set_env(rabbitmq_queue_master_balancer, sync_delay_timeout, ?TEST_SYNC_DELAY_TIMEOUT),
  application:set_env(rabbitmq_queue_master_balancer, sync_verification_factor, ?TEST_SYNC_VERIFICATION_FACTOR),
  application:set_env(rabbitmq_queue_master_balancer, master_verification_timeout, ?TEST_MASTER_VERIFICATION_TIMEOUT),
  application:set_env(rabbitmq_queue_master_balancer, policy_transition_delay, ?TEST_POLICY_TRANSITION_DELAY),

  ok = application:start(rabbitmq_queue_master_balancer).

is_pid_alive(Config, Pid) ->
  rabbit_ct_broker_helpers:rpc(Config, node(Pid), erlang, is_process_alive, [Pid]).

get_node_names(Config, ClusterSize) ->
  [rabbit_ct_broker_helpers:rpc(Config, Node-1,
    erlang, node, []) || Node <- lists:seq(1, ClusterSize)].

occurance(N, Nodes) ->
 occurance(N, Nodes, 0).

occurance(_N,[], C)      -> C;
occurance(N, [N|Rem], C) -> occurance(N, Rem, C+1);
occurance(N, [_|Rem], C) -> occurance(N, Rem, C).

info(Config, N) ->
  fire_fsm_event(Config, N, info, [infinity]).

status(Config, N) ->
  fire_fsm_event(Config, N, status).

report(Config, N) ->
  fire_fsm_event(Config, N, report, [infinity]).

load_queues(Config, N) ->
  fire_fsm_event(Config, N, load_queues).

balance_queues(Config, N) ->
  fire_fsm_event(Config, N, go).

pause(Config, N) ->
  fire_fsm_event(Config, N, pause).

continue(Config, N) ->
  fire_fsm_event(Config, N, continue).

reset(Config, N) ->
  fire_fsm_event(Config, N, reset).

stop(Config, N) ->
  fire_fsm_event(Config, N, stop).

fire_fsm_event(Config, N, Cmd) ->
    fire_fsm_event(Config, N, Cmd, []).
fire_fsm_event(Config, N, Cmd, Args) ->
  rabbit_ct_broker_helpers:rpc(Config, N,
    rabbit_queue_master_balancer, Cmd, Args).

verify_ha(Config) ->
  Cluster = get_node_names(Config, 3),
  QNs     = get_queue_names_and_nodes(Config),
  [passed = assert_slaves(Config, 0, QN, {N, Cluster--[N]}) || {QN, N} <- QNs],
  passed.

assert_slaves(Config, RPCNode, QName, {ExpMNode, ExpSNodes}) ->
  Q = find_queue(Config, QName, RPCNode),
  Pid = pget(pid, Q),
  SPids = pget(slave_pids, Q),
  ActMNode = node(Pid),
  ActSNodes = case SPids of
                  '' -> '';
                  _  -> [node(SPid) || SPid <- SPids]
              end,
  case ExpMNode =:= ActMNode andalso equal_list(ExpSNodes, ActSNodes) of
      false -> fail;
      true  -> passed
  end.

equal_list('',    '')   -> true;
equal_list('',    _Act) -> false;
equal_list(_Exp,  '')   -> false;
equal_list([],    [])   -> true;
equal_list(_Exp,  [])   -> false;
equal_list([],    _Act) -> false;
equal_list([H|T], Act)  ->
  case lists:member(H, Act) of
    true  -> equal_list(T, Act -- [H]);
    false -> false
  end.

find_queue(Config, QName, RPCNode) ->
  Qs = rabbit_ct_broker_helpers:rpc(Config, RPCNode, rabbit_amqqueue, info_all,
         [?VHOST], infinity),
  case find_queue0(QName, Qs) of
      did_not_find_queue -> timer:sleep(100),
                            find_queue(Config, QName, RPCNode);
      Q -> Q
  end.

find_queue0(QName, Qs) ->
  case [Q || Q <- Qs, pget(name, Q) =:=
                 rabbit_misc:r(?VHOST, queue, QName)] of
      [R] -> R;
      []  -> did_not_find_queue
  end.

get_pid(Config) ->
 rabbit_ct_broker_helpers:rpc(Config, 0,
      erlang, whereis, [rabbit_queue_master_balancer]).

verify_messages(Config, Node, ExpectedMessageCount) ->
  [
    [{messages, ExpectedMessageCount}] =
      rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, info,
        [Q, [messages]])
   || Q <- rabbit_ct_broker_helpers:rpc(Config, Node, rabbit_amqqueue, list, [])
  ],
  passed.

delay(T) -> timer:sleep(T).

%% CT tests will timeout and fail if completion is prolonged!
wait_until_complete(Config, A, T) ->
    case pget(phase, Info = info(Config, A)) of
        ?STATE_IDLE -> {ok, Info};
        _ ->
            delay(T),
            wait_until_complete(Config, A, T)
    end.

floor_value(V) -> math:floor(V).

calc_balanced(Threshold, QArgs) ->
  ?MIN_MASTERS_FILTER(QArgs, fun() -> 0 end, fun() -> floor_value(Threshold * 2) end).
