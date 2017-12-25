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
%% Copyright (c) 2017-2018 Erlang Solutions, Ltd.  All rights reserved.
%%

-module(rabbit_queue_master_balancer).
-behaviour(gen_fsm).

-export([start_link/0, load_queues/0, go/0, pause/0, continue/0,
         info/0, reset/0, report/0, stop/0, shutdown/0]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         code_change/4, terminate/3]).

-export([idle/2, ready/2, balancing_queues/2, pause/2]).

-include("rabbit_queue_master_balancer.hrl").

% ------------------------------------------------------------
-type report_entry() :: {node(), {'queues', integer()}}.
-type report()       :: {ok, [report_entry()]}.

-spec start_link()  -> rabbit_types:ok_pid_or_error().
-spec load_queues() -> 'ok'.
-spec go()          -> 'ok'.
-spec pause()       -> 'ok'.
-spec continue()    -> 'ok'.
-spec info()        -> 'ok'.
-spec report()      -> report().
-spec reset()       -> 'ok'.
-spec stop()        -> 'ok'.
-spec shutdown()    -> 'ok'.
% ------------------------------------------------------------

-record(state, {parent_pid,
                phase,
                queues       = [],
                balanced     = [],
                op_priority,
                sync_timeout,
                policy_trans_delay,
                balance_ts}).

%% --------
%% FSM API
%% --------
start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, [self()], []).

load_queues() ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$load_queues').

go() ->
  gen_fsm:send_event(?MODULE, '$balance_queues').

pause() ->
  gen_fsm:send_event(?MODULE, '$pause').

continue() ->
  gen_fsm:send_event(?MODULE, '$continue').

info() ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$info').

report() ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$report').

reset() ->
  gen_fsm:send_all_state_event(?MODULE, '$reset').

stop() ->
  gen_fsm:send_all_state_event(?MODULE, '$stop').

shutdown() ->
  gen_fsm:stop(?MODULE).

%% -------------------------
%% FSM (Mandatory) CallBacks
%% -------------------------
init([Parent]) ->
  process_flag(trap_exit, true),
  InitQueues   = init_queues(),
  OpPriority   = rabbit_misc:get_env(?MODULE, operational_priority,
  	               ?DEFAULT_OPERATIONAL_PRIORITY),
  SynchTimeout = rabbit_misc:get_env(?MODULE, sync_delay_timeout,
  	               ?DEFAULT_SYNC_DELAY_TIMEOUT),
  PTD          = get_policy_trans_delay(),
  {ok, ?STATE_IDLE, #state{parent_pid         = Parent,
                           phase              = ?STATE_IDLE,
                           queues             = InitQueues,
                           op_priority        = OpPriority,
                           sync_timeout       = SynchTimeout,
                           policy_trans_delay = PTD}}.

handle_sync_event('$load_queues', _From, _StateName, State) ->
  Queues = fetch_queues(),
  error_logger:info_msg("Queue Master Balancer loading ~p queues~n",
                        [Count = length(Queues)]),
  {reply, {ok, Count}, ?STATE_READY,
                       State#state{queues    = Queues,
                                   phase     = ?STATE_READY}};
handle_sync_event('$info', _From, StateName, State) ->
  Reply = to_info(State),
  {reply, Reply, StateName, State};
handle_sync_event('$report', _From, StateName, State) ->
  Reply = make_report(),
  {reply, {ok, Reply}, StateName, State}.

handle_event('$reset', _StateName, State) ->
  {next_state, ?STATE_IDLE, State#state{queues    = [],
                                        balanced  = [],
                                        phase     = ?STATE_IDLE}};
handle_event('$stop', _StateName, State) ->
  {next_state, ?STATE_IDLE, State#state{phase = ?STATE_IDLE}}.

handle_info(_Info, StateName, State) ->
  {next_state, StateName, State}.

code_change(_OldVsn, _StateName, State, _Extra) ->
  {ok, idle, State}.

terminate(_Reason, _StateName, _State) ->
  ok.

%% ----------
%% FSM States
%% ----------
idle('$balance_queues', State = #state{queues = Queues}) ->
  error_logger:info_msg("Queue Master Balancer balancing ~p queues~n",
                        [length(Queues)]),
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES, State#state{phase=?STATE_BALANCING_QUEUES}};
idle(_Event, State = #state{phase = ?STATE_IDLE}) ->
  {next_state, ?STATE_IDLE, State}.

ready('$balance_queues', State) ->
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES, State#state{phase=?STATE_BALANCING_QUEUES}};
ready(_Event, State = #state{phase = ?STATE_READY}) ->
  {next_state, ?STATE_READY, State}.

balancing_queues('$balance_queues', State = #state{queues = []}) ->
  {next_state, ?STATE_IDLE, State#state{phase = ?STATE_IDLE}};
balancing_queues('$balance_queues',
	               State = #state{queues             = [Q|Queues],
	                              op_priority        =  OpPriority,
	                              balanced           =  Balanced,
	                              sync_timeout       =  SynchTimeout,
	                              policy_trans_delay =  PTD}) ->
  {ok, QName} = balance_queue(Q, OpPriority, PTD, SynchTimeout),
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES,
    State#state{queues     = Queues,
                balanced   = sets:to_list(sets:from_list([QName|Balanced])),
                phase      = ?STATE_BALANCING_QUEUES,
                balance_ts = ts()}};
balancing_queues('$pause', State = #state{queues = Queues, balanced = B}) ->
  error_logger:info_msg("Queue Master Balancer paused. ~p queues pending "
                        "and ~p queues balanced", [length(Queues), length(B)]),
  {next_state, ?STATE_PAUSE, State#state{phase = ?STATE_PAUSE}};
balancing_queues(_Event, State = #state{phase = ?STATE_BALANCING_QUEUES}) ->
  {next_state, ?STATE_BALANCING_QUEUES, State}.

pause('$continue', State = #state{queues = Queues}) ->
  error_logger:info_msg("Queue Master Balancer continuing: "
                        "~p pending queues~n", [length(Queues)]),
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES,
    State#state{phase = ?STATE_BALANCING_QUEUES}};
pause(_Event, State = #state{phase = ?STATE_PAUSE}) ->
  {next_state, ?STATE_PAUSE, State}.

% --------
% Internal
% --------
init_queues() ->
  PreloadQueues = rabbit_misc:get_env(?MODULE, preload_queues, false),
  if PreloadQueues -> fetch_queues();
  	true -> []
  end.

to_info(#state{parent_pid         = PPid,
               phase              = Phase,
               queues             = Queues,
               balanced           = BalancedQueues,
               op_priority        = Priority,
               sync_timeout       = SynchTimeout,
               policy_trans_delay = PTD,
               balance_ts         = TS}) ->
  [{parent_pid,              PPid},
   {phase,                   Phase},
   {queues,                  Queues},
   {balanced,                BalancedQueues},
   {operational_priority,    Priority},
   {sync_timeout,            SynchTimeout},
   {policy_transition_delay, PTD},
   {last_balance_timestamp,  TS}].

balance_queue(Q, Priority, PTD, SynchTimeout) ->
    %% Aqcuire Min-master
    {ok, MinMaster} =
	  rabbit_queue_location_min_masters:queue_master_location(Q),
    try
       User      = get_acting_user(Q),
       {ok, _QN} = shuffle_queue(Q, MinMaster, Priority, PTD, SynchTimeout, User)
	catch
	   _:Reason -> {error, Reason}
	end.

%% 3.6.0 <--> 3.6.5
shuffle_queue(Q = {amqqueue, {resource, VHost, queue, QName},_,_,_,_,QPid,SPids,_,_,
	Policy,_,_,live}, MinMaster, Priority, PTD, SynchTimeout, User) ->
    shuffle(VHost, QName, Policy, MinMaster, QPid, SPids, Priority, PTD,
      SynchTimeout, User, messages(Q));
shuffle_queue({amqqueue, {resource, _, queue, QName},_,_,_,_,_,_,_,_,_,_,_,_},
  _MinMaster, _Priority, _PTD, _SynchTimeout, _User) ->
      {ok, QName};

%% 3.6.6 <--> 3.6.x
shuffle_queue(Q = {amqqueue, {resource, VHost, queue, QName},_,_,_,_,QPid,SPids,_,_,
	Policy,_,_,live,_}, MinMaster, Priority, PTD, SynchTimeout, User) ->
    shuffle(VHost, QName, Policy, MinMaster, QPid, SPids, Priority, PTD,
      SynchTimeout, User, messages(Q));
shuffle_queue({amqqueue, {resource, _, queue, QName},_,_,_,_,_,_,_,_,_,_,_,_,_},
  _MinMaster, _Priority, _PTD, _SynchTimeout, _User) ->
      {ok, QName};

%% 3.7.0 --> ...
shuffle_queue(Q = {amqqueue,{resource, VHost, queue, QName},_,_,_,_,QPid,SPids,_,_,
  Policy,_,_,_,live,_,_,_,_}, MinMaster, Priority, PTD, SynchTimeout, User) ->
    shuffle(VHost, QName, Policy, MinMaster, QPid, SPids, Priority, PTD,
      SynchTimeout, User, messages(Q));
shuffle_queue({amqqueue,{resource, _VHost, queue, QName},_,_,_,_,_,_,_,_,_,_,_,
  _,_,_,_,_,_}, _MinMaster, _Priority, _PTD, _SynchTimeout, _User) ->
      {ok, QName};

%% Unsupported version
shuffle_queue(Q, _MinMaster, _Priority, _PTD, _SynchTimeout, _VSNComp) ->
  throw({unsupported_version, Q}).

shuffle(_, QN, _, _, _, _SPids = [], _, _, _, _User, M) when M > 0 ->
{ok, QN};
shuffle(VHost, QN, Policy, MinMaster, _QPid, SPids, Priority, PTD, SynchTimeout,
    User, _M) ->
  Pattern = list_to_binary(lists:concat(["^", binary_to_list(QN), "$"])),
  ok = rabbit_queue_master_balancer_sync:sync_mirrors(SPids, get_queue(VHost, QN)),
  ok = policy_transition_delay(PTD),
  ok = set_policy(VHost, QN, Pattern, [{<<"ha-mode">>, <<"nodes">>},{<<"ha-params">>,
         [list_to_binary(atom_to_list(MinMaster))]}], Priority, <<"queues">>, User),
  ok = policy_transition_delay(PTD),
  ok = delete_policy(VHost, QN, User),
  ok = policy_transition_delay(PTD),
  ok = reset_policy(Policy, PTD, User),
  ok = policy_transition_delay(PTD),
  ok = rabbit_queue_master_balancer_sync:sync_mirrors(SPids, get_queue(VHost, QN)),
  try
  	  rabbit_queue_master_balancer_sync:verify_sync(VHost, QN, SPids, SynchTimeout)
  catch
  	_:Reason ->
  	  error_logger:error_msg("Queue Master Balancer synchronisation error."
                             "~nQueue: ~p~nReason: ~p~n", [QN, Reason]),
  	  void
  end,
  {ok, QN}.

set_policy(VHost, QN, Pattern, Spec, Priority, ApplyTo, undefined) ->
  rabbit_policy:set(VHost, QN, Pattern, Spec, Priority, ApplyTo);
set_policy(VHost, QN, Pattern, Spec, Priority, ApplyTo, User) ->
  rabbit_policy:set(VHost, QN, Pattern, Spec, Priority, ApplyTo, User).

reset_policy(undefined, _PTD, _User) -> ok;
reset_policy(Policy, PTD, User) ->
  VHost    = rabbit_misc:pget(vhost, Policy),
  Name     = rabbit_misc:pget(name, Policy),
  Pattern  = rabbit_misc:pget(pattern, Policy),
  Def      = rabbit_misc:pget(definition, Policy),
  Priority = rabbit_misc:pget(priority, Policy),
  ApplyTo  = rabbit_misc:pget('apply-to', Policy),
  ok = delete_policy(VHost, Name, User),
  ok = policy_transition_delay(PTD),
  ok = set_policy(VHost, Name, Pattern, Def, Priority, ApplyTo, User).

delete_policy(VHost, QN, undefined) -> rabbit_policy:delete(VHost, QN);
delete_policy(VHost, QN, User)      -> rabbit_policy:delete(VHost, QN, User).

fetch_queues() -> rabbit_amqqueue:list().

make_report() ->
  QNs = lists:foldl(fun(Q, Acc) -> [get_queue_node(Q)|Acc] end, [], fetch_queues()),
  [count(N, QNs, 0) || N <- rabbit_mnesia:cluster_nodes(running)].

count(N, [], C)      -> {N, {queues, C}};
count(N, [N|Rem], C) -> count(N, Rem, C+1);
count(N, [_|Rem], C) -> count(N, Rem, C).

get_queue(VHost, QN) ->
  {ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(VHost, queue, QN)),
  Q.

get_queue_node(Q) ->
  node(case Q of
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live} ->
            Pid;
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live,_} ->
            Pid;
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,_,live,_,_,_,_} ->
            Pid;
          Other -> error({unsupported_version, Other})
       end).

ts() ->
  {Mega, Sec, USec} = os:timestamp(),
  (Mega * 1000000 + Sec) * 1000 + round(USec/1000).

get_policy_trans_delay() ->
  case rabbit_misc:get_env(?MODULE, policy_transition_delay,
          ?DEFAULT_POLICY_TRANSITION_DELAY) of
    PTD when is_integer(PTD); PTD >= ?DEFAULT_POLICY_TRANSITION_DELAY -> PTD;
    _ ->
      error_logger:info_msg("Queue Master Balancer setting default "
        "policy transition delay: ~pms", [?DEFAULT_POLICY_TRANSITION_DELAY]),
      ?DEFAULT_POLICY_TRANSITION_DELAY
  end.

policy_transition_delay(PTD) -> timer:sleep(PTD).

messages(Q) ->
  [{messages, Messages}] = rabbit_amqqueue:info(Q, [messages]),
    Messages.

get_acting_user(Q) ->
  case rabbit_misc:version_compare(rabbit_misc:version(), "3.7.0") of
    lt -> undefined;
    _  ->
      {amqqueue,{resource, _VHost, queue, _QN},_,_,_,_,_,_,_,_,_,_,_,_,live,
      _,_,_, User} = Q,
      User
  end.
