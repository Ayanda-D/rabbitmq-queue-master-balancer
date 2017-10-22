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

-export([sync_mirrors/2, verify_sync/3, verify_sync/4]).

-include("rabbit_queue_master_balancer.hrl").

% ---------------------------------------------------------------
-spec sync_mirrors(list(), rabbit_types:amqqueue() | pid()) -> 'ok'.
-spec verify_sync(binary(), binary(), list())               -> 'ok'.
-spec verify_sync(binary(), binary(), list(), integer())    -> 'ok'.
% ----------------------------------------------------------------

sync_mirrors([], _Q)    -> ok;
sync_mirrors(_SPids, Q) ->
  _Any = rabbit_amqqueue:sync_mirrors(Q),
  ok.

verify_sync(VHost, QN, SPids) ->
  verify_sync(VHost, QN, SPids, ?DEFAULT_SYNC_DELAY_TIMEOUT).

verify_sync(VHost, QN, SPids, Timeout) ->
  SynchPPid = self(),
  SynchRef  = make_ref(),
  Syncher   = spawn_link(fun() ->
                           verify_sync(VHost, SynchPPid, SynchRef, QN, SPids)
                         end),
  receive
    {Syncher, _SyncherRef, done} -> ok;
    {'EXIT',  _Syncher, Reason}  -> throw(Reason)
  after Timeout ->
    exit(Syncher, {timeout, ?MODULE})
  end.

verify_sync(VHost, SynchPPid, SynchRef, QN, SPids) -> 
  SSPs = length(synchronised_slave_pids(VHost, QN)),
  if SSPs =:= length(SPids) -> SynchPPid ! {self(), SynchRef, done};
     true -> verify_sync(VHost, SynchPPid, SynchRef, QN, SPids)
  end.

synchronised_slave_pids(VHost, Queue) ->
    {ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(VHost, queue, Queue)),
    SSP = synchronised_slave_pids,
    [{SSP, Pids}] = rabbit_amqqueue:info(Q, [SSP]),
    case Pids of
        '' -> [];
        _  -> Pids
    end.