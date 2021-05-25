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

-module(rabbit_queue_master_balancer_sync).

-export([sync_mirrors/1, verify_sync/3, verify_sync/4, verify_sync/5,
         verify_sync/7]).
-export([verify_master/2, verify_master/3, verify_master/4,
         verify_master/6]).

-include("rabbit_queue_master_balancer.hrl").

% ---------------------------------------------------------------
-spec sync_mirrors(rabbit_types:amqqueue() | pid()) -> 'ok'.
-spec verify_sync(binary(), binary(), list())               -> 'ok'.
-spec verify_sync(binary(), binary(), list(), integer())    -> 'ok'.
% ----------------------------------------------------------------

sync_mirrors(Q) ->
  _Any = safe_queue_call(Q, fun() -> rabbit_amqqueue:sync_mirrors(Q) end, ok),
  ok.

verify_sync(VHost, QN, SPids) ->
  verify_sync(VHost, QN, SPids, ?DEFAULT_SYNC_DELAY_TIMEOUT).
verify_sync(VHost, QN, SPids, Timeout) ->
  verify_sync(VHost, QN, SPids, Timeout, ?DEFAULT_SYNC_VERIFICATION_FACTOR).
verify_sync(VHost, QN, SPids, Timeout, DelayFactor) ->
  SynchPPid = self(),
  SynchRef  = make_ref(),
  Syncher   = spawn(fun() ->
                        verify_sync(VHost, SynchPPid, SynchRef, 0, DelayFactor,
                                    QN, SPids)
                    end),
  receive
    {Syncher, _SyncherRef, done} -> ok;
    {'EXIT',  _Syncher, Reason}  -> throw({sync_termination, Reason})
  after Timeout ->
    exit(Syncher, {timeout, ?MODULE})
  end.

verify_sync(VHost, SynchPPid, SynchRef, SynchPeriod, DelayFactor, QN, SPids) ->
  {SSPids, Msgs} = synchronised_slave_pids(VHost, QN, SynchPeriod),
  SynchPeriod0 = ?UPDATE_RELATIVE(Msgs, DelayFactor, ?SYNC_THRESHOLD, ?DEFAULT_SYNC_VERIFICATION_FACTOR),
  if length(SSPids) =:= length(SPids) -> SynchPPid ! {self(), SynchRef, done};
     true -> verify_sync(VHost, SynchPPid, SynchRef, SynchPeriod0, DelayFactor, QN, SPids)
  end.

synchronised_slave_pids(VHost, Queue, SynchPeriod) ->
    ?DELAY(SynchPeriod),
    QMetrics = rabbit_queue_master_balancer:queue_metrics(
                   rabbit_misc:r(VHost, queue, Queue)),
    Pids = rabbit_misc:pget(synchronised_slave_pids, QMetrics, []),
    Msgs = rabbit_misc:pget(messages_ram, QMetrics, 0),
    {Pids, Msgs}.

verify_master(VHost, QN) ->
    verify_master(VHost, QN, ?DEFAULT_MASTER_VERIFICATION_TIMEOUT).
verify_master(VHost, QN, Timeout) ->
    verify_master(VHost, QN, Timeout, ?DEFAULT_SYNC_VERIFICATION_FACTOR).
verify_master(VHost, QN, Timeout, DelayFactor) ->
  VerifierPPid = self(),
  VerifierRef  = make_ref(),
  Verifier     = spawn(fun() ->
                        verify_master(VHost, VerifierPPid, VerifierRef,
                                      0, DelayFactor, QN)
                    end),
  receive
    {Verifier, _VerifierRef, alive} -> ok;
    {'EXIT', _VerifierRef, Reason}  -> throw({verify_master_termination, Reason})
  after Timeout ->
    exit(Verifier, {verify_master_timeout, ?MODULE})
  end.

verify_master(VHost, VerifierPPid, VerifierRef, VerifierPeriod, X, QN) ->
  {IsQMasterAlive, VerifierPeriod0} = is_queue_master_alive(VHost, QN, VerifierPeriod, X),
  if IsQMasterAlive -> VerifierPPid ! {self(), VerifierRef, alive};
     true -> verify_master(VHost, VerifierPPid, VerifierRef, VerifierPeriod0, X, QN)
  end.

is_queue_master_alive(VHost, Queue, VerifierPeriod, X) ->
    ?DELAY(VerifierPeriod),
    QResource = rabbit_misc:r(VHost, queue, Queue),
    {ok, Q}   = rabbit_amqqueue:lookup(QResource),
    QMetrics  = rabbit_queue_master_balancer:queue_metrics(QResource),
    Msgs = rabbit_misc:pget(messages_ram, QMetrics, 0),
    VerifierPeriod0 = ?UPDATE_RELATIVE(Msgs, X, ?SYNC_THRESHOLD, ?DEFAULT_SYNC_VERIFICATION_FACTOR),
    {Pid, State} = get_pid_and_state(Q),
    {is_pid_alive(Pid) andalso (State =:= live), VerifierPeriod0}.

%% Queue process can now be existing on remote node after migration operations
is_pid_alive(Pid) when is_pid(Pid) ->
    rpc:call(node(Pid), erlang, is_process_alive, [Pid]).

get_pid_and_state(AMQQueue) ->
  case AMQQueue of
      {amqqueue, {resource, _, queue, _},_,_,_,_,Pid,_,_,_,_,_,_,State} ->
         {Pid, State};
      {amqqueue, {resource, _, queue, _},_,_,_,_,Pid,_,_,_,_,_,_,State,_} ->
         {Pid, State};
      {amqqueue,{resource, _, queue, _},_,_,_,_,Pid,_,_,_,_,_,_,_,State,_,_,_,_} ->
         {Pid, State};
      Other -> error({unsupported_version, Other})
  end.

safe_queue_call(AMQQueue, OpFun, Default) ->
  {Pid, _State} = get_pid_and_state(AMQQueue),
  case is_process_alive(Pid) of
      true  -> OpFun();
      false -> Default
  end.
