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

-module(rabbit_queue_master_balancer_app).
-behaviour(application).

-export([start/0, start/2]).
-export([stop/0, stop/1]).

start()           -> start(normal, []).

stop()            -> stop([]).

start(normal, []) -> rabbit_queue_master_balancer_sup:start_link().

stop(_State)      -> rabbit_queue_master_balancer:shutdown(), ok.
