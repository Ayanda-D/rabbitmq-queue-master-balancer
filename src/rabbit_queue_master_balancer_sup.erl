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

-module(rabbit_queue_master_balancer_sup).
-behaviour(supervisor2).

-export([start_link/0, init/1]).

-include("rabbit_queue_master_balancer.hrl").

start_link() ->
    supervisor2:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ChildSpecs = [{rabbit_queue_master_balancer,
                  {rabbit_queue_master_balancer, start_link, []},
                   transient, 16#ffffffff, worker,
                  [rabbit_queue_master_balancer]}],
    {ok, {{one_for_one,3,10}, ChildSpecs}}.
