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

all() ->
    [
      {group, non_parallel_state_tests}
    ].

groups() ->
    [
      {non_parallel_state_tests, [], [
          successful_queue_balancing_no_ha_no_messages,
          successful_queue_balancing_with_ha_no_messages,
          successful_queue_balancing_with_ha_and_messages,
          unsuccessful_queue_balancing_no_ha_with_messages
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

%% ------------
%% Test Cases
%% ------------
successful_queue_balancing_no_ha_no_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 0,
  Delay   = 1000,
  init_queues(Config, 0, Queues, Messages),
  passed  = successful_run(Config, Queues, Nodes, Delay).

successful_queue_balancing_with_ha_no_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 0,
  Delay   = 1000,
  init_queues(Config, 0, Queues, Messages),
  passed  = successful_run(Config, Queues, Nodes, Delay),
  verify_ha(Config).

successful_queue_balancing_with_ha_and_messages(Config) ->
  [A|_]  = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues   = 4,
  Messages = 10,
  Delay    = 1000,
  init_queues(Config, 0, Queues, Messages),
  passed = successful_run(Config, Queues, Nodes, Delay),
  verify_ha(Config),
  verify_messages(Config, 0, Messages).

unsuccessful_queue_balancing_no_ha_with_messages(Config) ->
  [A|_]   = Nodes = get_node_names(Config, 3),
  setup_queue_master_balancer(Config, A),
  Queues  = 4,
  Messages= 10,
  Delay   = 50,
  init_queues(Config, 0, Queues, Messages),
  passed  = unsuccessful_run(Config, Queues, Nodes, Delay),
  verify_messages(Config, 0, Messages).


successful_run(Config, N, [A, B, C], Delay) ->
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
  Info1   = info(Config, A),

  %% Balance Queues: Info2
  ok    = balance_queues(Config, A),
  Info2 = info(Config, A),

  %% Balance Pause: Info3
  ok    = pause(Config, A),
  Info3 = info(Config, A),

  %% Balance Continue: Info4
  ok    = continue(Config, A),
  Info4 = info(Config, A),

  %% Wait for Balancing Complete
  delay(Delay),

  %% Balancing Complete: Info5
  Info5 = info(Config, A),

  %% Stop: Info6
  ok    = stop(Config, A),
  Info6 = info(Config, A),

  %% Reset: Info7
  ok    = reset(Config, A),
  Info7 = info(Config, A),

  %% Verify Idle Info0
  ?STATE_IDLE = pget(phase, Info0),
  false = (length(pget(queues,   Info0)) > 0), 
  false = (length(pget(balanced, Info0)) > 0), 

  %% Verify Load Queues Info1
  ?STATE_READY = pget(phase, Info1),
  true = (length(pget(queues,   Info1)) == N), 
  false= (length(pget(balanced, Info1)) >  0), 

  %% Verify Info2
  ?STATE_BALANCING_QUEUES = pget(phase, Info2),
  true = (length(pget(queues,   Info2)) > 0), 
  true = (length(pget(balanced, Info2)) > 0), 

  %% Verify Info3
  ?STATE_PAUSE = pget(phase, Info3),
  true = (length(pget(queues,   Info3)) > 0), 
  true = (length(pget(balanced, Info3)) > 0), 

  %% Verify Info4
  ?STATE_BALANCING_QUEUES = pget(phase, Info4),
  true = (length(pget(queues,   Info4)) > 0), 
  true = (length(pget(balanced, Info4)) > 0), 

  %% Verify Balancing Complete: Info5
  ?STATE_IDLE = pget(phase, Info5),
  true = (length(pget(queues,   Info5)) ==0), 
  true = (length(pget(balanced, Info5)) ==N), 

  %% Verify Stop: Info6
  ?STATE_IDLE = pget(phase, Info6),
  true = (length(pget(queues,   Info6)) ==0), 
  true = (length(pget(balanced, Info6)) ==N),

  %% Verify Reset: Info7
  ?STATE_IDLE = pget(phase, Info7),
  true = (length(pget(queues,   Info7)) ==0), 
  true = (length(pget(balanced, Info7)) ==0),

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

%% ---------
%% Internal
%% ---------
init_queues(Config, Node, NQs, NMsgs) ->
  Ch = rabbit_ct_client_helpers:open_channel(Config, Node),
  setup_exchange(Ch),
  declare_queues(Ch, NQs),
  publish(Ch, NMsgs).

setup_exchange(Ch) ->
    XD =  #'exchange.declare'{exchange = ?EXHANGE,
                              type = <<"fanout">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, XD).

declare_queues(Ch, NQs) ->
 [begin
    QN = list_to_binary("test.q."++integer_to_list(N)),
    QD = #'queue.declare'{queue = QN},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, QD),

    #'queue.bind_ok'{} =
        amqp_channel:call(Ch, #'queue.bind'{queue       = QN,
                                            exchange    = ?EXHANGE,
                                            routing_key = ?RK})
  end || N <- lists:seq(1, NQs)],
  ok.

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
            Pid
        end) || Q <- rabbit_amqqueue:list()].

get_queue_names_and_nodes(Config) ->
  [begin
    {QN0, Pid0} = 
      case Q of
        {amqqueue, {resource,?VHOST,queue,QN},_,_,_,_,Pid,_,_,_,_,_,_,live} ->
          {QN, Pid};
        {amqqueue, {resource,?VHOST,queue,QN},_,_,_,_,Pid,_,_,_,_,_,_,live,_} ->
          {QN, Pid}
      end,
    {QN0, node(Pid0)}
   end || Q <- rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, list, [])].

setup_queue_master_balancer(Config, Node) ->
  ok = rabbit_ct_broker_helpers:rpc(Config, Node,
         ?MODULE, init_queue_master_balancer_remote, []).

init_queue_master_balancer_remote() ->
  application:set_env(rabbitmq_queue_master_balancer, operational_priority, 15),
  application:set_env(rabbitmq_queue_master_balancer, preload_queues, false),
  application:set_env(rabbitmq_queue_master_balancer, sync_delay_timeout, 100),
  application:set_env(rabbitmq_queue_master_balancer, policy_transition_delay, 10),

  ok = application:start(rabbitmq_queue_master_balancer).

get_node_names(Config, ClusterSize) ->
  [rabbit_ct_broker_helpers:rpc(Config, Node-1,
    erlang, node, []) || Node <- lists:seq(1, ClusterSize)].

occurance(N, Nodes) ->
 occurance(N, Nodes, 0).

occurance(_N,[], C)      -> C;
occurance(N, [N|Rem], C) -> occurance(N, Rem, C+1);
occurance(N, [_|Rem], C) -> occurance(N, Rem, C).

info(Config, N) ->
  fire_fsm_event(Config, N, info).

report(Config, N) ->
  fire_fsm_event(Config, N, report).

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
  rabbit_ct_broker_helpers:rpc(Config, N,
    rabbit_queue_master_balancer, Cmd, []).

verify_ha(Config) ->
  Cluster = get_node_names(Config, 3),
  QNs     = get_queue_names_and_nodes(Config),
  [passed = assert_slaves(Config, 0, QN, {N, Cluster--[N]}) || {QN, N} <- QNs].

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
  ].

 delay(T) -> timer:sleep(T).