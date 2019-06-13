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
-compile(nowarn_deprecated_function).

-export([start_link/0, load_queues/0, load_queues/1, go/0, pause/0, continue/0,
         info/0, info/1, reset/0, status/0, report/0, report/1, stop/0, shutdown/0]).

-export([init/1, handle_event/3, handle_sync_event/4, handle_info/3,
         code_change/4, terminate/3]).

-export([idle/2, ready/2, balancing_queues/2, pause/2]).

-include("rabbit_queue_master_balancer.hrl").

% ------------------------------------------------------------
-type report_entry() :: {node(), {'queues', integer()}}.
-type report()       :: {ok, [report_entry()]}.
-type status()       :: [{atom(), term()}].

-spec start_link()  -> rabbit_types:ok_pid_or_error().
-spec load_queues() -> 'ok'.
-spec go()          -> 'ok'.
-spec pause()       -> 'ok'.
-spec continue()    -> 'ok'.
-spec info()        -> 'ok'.
-spec status()      -> status().
-spec report()      -> report().
-spec reset()       -> 'ok'.
-spec stop()        -> 'ok'.
-spec shutdown()    -> 'ok'.
% ------------------------------------------------------------

-record(state, {parent_pid,
                phase,
                position     = 0,
                prev         = undefined,
                size_t       = 0,
                balanced     = 0,
                op_priority,
                sync_timeout,
                policy_trans_delay,
                balance_ts}).

-define(TAB,       ?MODULE).
-define(SIZE,      ets:info(?TAB, size)).
-define(FIRST,     ets:first(?TAB)).
-define(DEFAULT_ALL_STATE_EVENT_CALL_TIMEOUT,  5000).

%% --------
%% FSM API
%% --------
start_link() ->
  gen_fsm:start_link({local, ?MODULE}, ?MODULE, [self()], []).

load_queues() ->
  load_queues(?DEFAULT_ALL_STATE_EVENT_CALL_TIMEOUT).

load_queues(Timeout) ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$load_queues', Timeout).

go() ->
  gen_fsm:send_event(?MODULE, '$balance_queues').

pause() ->
  gen_fsm:send_event(?MODULE, '$pause').

continue() ->
  gen_fsm:send_event(?MODULE, '$continue').

info() ->
  info(?DEFAULT_ALL_STATE_EVENT_CALL_TIMEOUT).

info(Timeout) ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$info', Timeout).

report() ->
  report(?DEFAULT_ALL_STATE_EVENT_CALL_TIMEOUT).

report(Timeout) ->
  gen_fsm:sync_send_all_state_event(?MODULE, '$report', Timeout).

reset() ->
  gen_fsm:send_all_state_event(?MODULE, '$reset').

stop() ->
  gen_fsm:send_all_state_event(?MODULE, '$stop').

shutdown() ->
  gen_fsm:stop(?MODULE).

%% -------------------
%% FSM independant API
%% -------------------
status() -> fetch_current_status().

%% -------------------------
%% FSM (Mandatory) CallBacks
%% -------------------------
init([Parent]) ->
  process_flag(trap_exit, true),
  ?MODULE      = ets:new(?MODULE, [named_table, set, private]),
  ok           = init_queues(),
  OpPriority   = get_config(operational_priority, ?DEFAULT_OPERATIONAL_PRIORITY),
  SynchTimeout = get_config(sync_delay_timeout, ?DEFAULT_SYNC_DELAY_TIMEOUT),
  PTD          = get_policy_trans_delay(),
  {ok, ?STATE_IDLE, #state{parent_pid         = Parent,
                           phase              = ?STATE_IDLE,
                           position           = ?FIRST,
                           op_priority        = OpPriority,
                           sync_timeout       = SynchTimeout,
                           policy_trans_delay = PTD}}.

handle_sync_event('$load_queues', _From, _StateName, State = #state{}) ->
  ok = insert_queues(),
  error_logger:info_msg("Queue Master Balancer loading ~p queues~n",
                        [Count = ?SIZE]),
  {reply, {ok, Count}, ?STATE_READY,
                       State#state{position  = ?FIRST,
                                   size_t    = ?SIZE,
                                   prev      = undefined,
                                   balanced  = 0,
                                   phase     = ?STATE_READY}};
handle_sync_event('$info', _From, StateName, State) ->
  Reply = to_info(State),
  {reply, Reply, StateName, State};
handle_sync_event('$report', _From, StateName, State) ->
  Reply = make_report(),
  {reply, {ok, Reply}, StateName, State}.

handle_event('$reset', _StateName, State) ->
  ets:delete_all_objects(?TAB),
  {next_state, ?STATE_IDLE, State#state{position  = ?FIRST,
                                        prev      = undefined,
                                        balanced  = 0,
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
idle('$balance_queues', State = #state{}) ->
  error_logger:info_msg("Queue Master Balancer balancing ~p queues~n",
                        [?SIZE]),
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES, State#state{size_t = ?SIZE,
                                                    phase  = ?STATE_BALANCING_QUEUES}};
idle(_Event, State = #state{phase = ?STATE_IDLE}) ->
  {next_state, ?STATE_IDLE, State}.

ready('$balance_queues', State) ->
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES, State#state{size_t = ?SIZE,
                                                    phase  = ?STATE_BALANCING_QUEUES}};
ready(_Event, State = #state{phase = ?STATE_READY}) ->
  {next_state, ?STATE_READY, State}.

balancing_queues('$balance_queues', State = #state{balanced = Balanced,
                                                   prev     = Prev,
                                                   position = '$end_of_table'}) ->
  maybe_drop(Prev),
  error_logger:info_msg("Queue Master Balancer completed balancing ~p queues",
                        [Balanced]),
  {next_state, ?STATE_IDLE, State#state{prev = undefined, phase = ?STATE_IDLE}};
balancing_queues('$balance_queues',
                   State = #state{position           =  Pos,
                                  prev               =  Prev,
                                  balanced           =  Balanced,
                                  op_priority        =  OpPriority,
                                  sync_timeout       =  SynchTimeout,
                                  policy_trans_delay =  PTD}) ->
  case ets:lookup(?TAB, Pos) of
    [{Pos, {QName, VHost}}] ->
        {ok, QName} =
            balance_queue(get_queue(VHost, QName), OpPriority, PTD, SynchTimeout),
            maybe_drop(Prev);
    _ ->
        void
  end,
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES,
      State#state{position   = ets:next(?TAB, Pos),
                  prev       = Pos,
                  balanced   = Balanced + 1,
                  phase      = ?STATE_BALANCING_QUEUES,
                  balance_ts = ts()}};
balancing_queues('$pause', State = #state{size_t = OSize, balanced = B}) ->
  error_logger:info_msg("Queue Master Balancer paused. ~p queues pending "
                        "and ~p queues balanced", [OSize - B, B]),
  {next_state, ?STATE_PAUSE, State#state{phase = ?STATE_PAUSE}};
balancing_queues(_Event, State = #state{phase = ?STATE_BALANCING_QUEUES}) ->
  {next_state, ?STATE_BALANCING_QUEUES, State}.

pause('$continue', State = #state{size_t = OSize, balanced = B}) ->
  error_logger:info_msg("Queue Master Balancer continuing: "
                        "~p pending queues~n", [OSize - B]),
  gen_fsm:send_event(?MODULE, '$balance_queues'),
  {next_state, ?STATE_BALANCING_QUEUES,
    State#state{phase = ?STATE_BALANCING_QUEUES}};
pause(_Event, State = #state{phase = ?STATE_PAUSE}) ->
  {next_state, ?STATE_PAUSE, State}.

% --------
% Internal
% --------
init_queues() ->
  PreloadQueues = get_config(preload_queues, false),
  if PreloadQueues -> insert_queues();
    true           -> ok
  end.

insert_queues() ->
    lists:foldl(fun(Entry = {_Q, _V}, Acc) ->
                    true = ets:insert(?TAB, {Acc, Entry}),
                    Acc + 1
                end, 0, fetch_queue_ids()),
    ok.

to_info(#state{parent_pid         = PPid,
               phase              = Phase,
               position           = Pos,
               size_t             = Size,
               balanced           = Balanced,
               op_priority        = Priority,
               sync_timeout       = SynchTimeout,
               policy_trans_delay = PTD,
               balance_ts         = TS}) ->
  [{parent_pid,              PPid},
   {phase,                   Phase},
   {total,                   Size},
   {position,                to_pos(Pos)},
   {balanced,                Balanced},
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
shuffle(VHost, QN, Policy, MinMaster, _QPid, _SPids, Priority, PTD, SynchTimeout,
    User, _M) ->
  Pattern = list_to_binary(lists:concat(["^", binary_to_list(QN), "$"])),
  ok = policy_transition_delay(PTD),
  ok = ensure_sync(VHost, QN, SynchTimeout),
  ok = set_policy(VHost, QN, Pattern, [{<<"ha-mode">>, <<"nodes">>},{<<"ha-params">>,
         [list_to_binary(atom_to_list(MinMaster))]}], Priority, <<"queues">>, User),
  ok = policy_transition_delay(PTD),
  ok = ensure_sync(VHost, QN, SynchTimeout),
  ok = delete_policy(VHost, QN, User),
  ok = policy_transition_delay(PTD),
  ok = ensure_sync(VHost, QN, SynchTimeout),
  ok = reset_policy(Policy, PTD, User),
  ok = policy_transition_delay(PTD),
  ok = ensure_sync(VHost, QN, SynchTimeout),
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

fetch_queue_ids() -> [to_id(Q) || Q <- fetch_queues()].

to_id(Q) ->
    {resource, VHost, queue, QName} = element(2, Q),
    {QName, VHost}.

make_report() ->
  QNs = lists:foldl(fun(Q, Acc) -> [get_queue_node(Q)|Acc] end, [], fetch_queues()),
  [count(N, QNs, 0) || N <- rabbit_mnesia:cluster_nodes(running)].

count(N, [], C)      -> {N, {queues, C}};
count(N, [N|Rem], C) -> count(N, Rem, C+1);
count(N, [_|Rem], C) -> count(N, Rem, C).

fetch_current_status() ->
    Pid     = whereis(rabbit_queue_master_balancer),
    Pending = ets:info(rabbit_queue_master_balancer, size),
    {memory, Memory}  = process_info(Pid, memory),
    [{'process_id', Pid},
     {'queues_pending_balance', Pending},
     {'memory_utilization', Memory}].

get_queue(VHost, QN) ->
  {ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(VHost, queue, QN)),
  Q.

get_queue_node(AMQQueue) ->
  node(case AMQQueue of
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live} ->
            Pid;
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,live,_} ->
            Pid;
          {amqqueue, {resource, _,queue,_},_,_,_,_,Pid,_,_,_,_,_,_,_,live,_,_,_,_} ->
            Pid;
          Other -> error({unsupported_version, Other})
       end).

get_queue_slaves(AMQQueue) ->
  case AMQQueue of
      {amqqueue, {resource, _, queue, _},_,_,_,_,_,SPids,_,_,_,_,_,live} ->
         SPids;
      {amqqueue, {resource, _, queue, _},_,_,_,_,_,SPids,_,_,_,_,_,live,_} ->
         SPids;
      {amqqueue,{resource, _, queue, _},_,_,_,_,_,SPids,_,_,_,_,_,_,live,_,_,_,_} ->
         SPids;
    Other -> error({unsupported_version, Other})
  end.

ts() ->
  {Mega, Sec, USec} = os:timestamp(),
  (Mega * 1000000 + Sec) * 1000 + round(USec/1000).

to_pos('$end_of_table') -> undefined;
to_pos(Any)             -> Any.

get_policy_trans_delay() ->
  case get_config(policy_transition_delay, ?DEFAULT_POLICY_TRANSITION_DELAY) of
    PTD when is_integer(PTD); PTD >= ?DEFAULT_POLICY_TRANSITION_DELAY -> PTD;
    _ ->
      error_logger:info_msg("Queue Master Balancer setting default "
        "policy transition delay: ~pms", [?DEFAULT_POLICY_TRANSITION_DELAY]),
      ?DEFAULT_POLICY_TRANSITION_DELAY
  end.

policy_transition_delay(PTD) -> ?DELAY(PTD).

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

maybe_drop(undefined) -> void;
maybe_drop(Key)       -> ets:delete(?TAB, Key).

get_config(Tag, Default) ->
    rabbit_misc:get_env(rabbitmq_queue_master_balancer, Tag, Default).

ensure_sync(VHost, QN, SynchTimeout) ->
  try
      %% TODO: Distinct queue master verification timeout!
      ok = rabbit_queue_master_balancer_sync:verify_master(VHost, QN),
      AMQQueue = get_queue(VHost, QN),
      SPids = get_queue_slaves(AMQQueue),
      ok = rabbit_queue_master_balancer_sync:sync_mirrors(AMQQueue),
      ok = rabbit_queue_master_balancer_sync:verify_sync(VHost, QN, SPids, SynchTimeout)
  catch
      _:Reason ->
            error_logger:error_msg("Queue Master Balancer synchronisation error. "
                                   "Queue: ~p, Reason: ~p~n", [QN, Reason]),
            exit(Reason)
  end.
