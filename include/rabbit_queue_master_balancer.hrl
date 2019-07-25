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

-define(DEFAULT_OPERATIONAL_PRIORITY,    5).
-define(DEFAULT_POLICY_TRANSITION_DELAY, 50).
-define(DEFAULT_SYNC_DELAY_TIMEOUT,      3000).
-define(DEFAULT_SYNC_VERIFICATION_FACTOR,500).
-define(DEFAULT_MASTER_VERIFICATION_TIMEOUT, 300000).
-define(DELAY(T),                        timer:sleep(T)).
-define(SYNC_THRESHOLD,                  100).
-define(PTD_THRESHOLD,                   100).

-define(STATE_IDLE,             idle).
-define(STATE_READY,            ready).
-define(STATE_BALANCING_QUEUES, balancing_queues).
-define(STATE_PAUSE,            pause).
-define(MAX_QEQ,                100).
-define(MIN_QEQ,                50).

-define(UPDATE_RELATIVE(Msgs, DelayFactor, MsgThreshold, Default),
            (case {Msgs, DelayFactor, Default} of
                {Msgs, DelayFactor, Default} when Msgs =< MsgThreshold,
                                                  DelayFactor < Default ->
                    Default;
                {Msgs, DelayFactor, Default} when Msgs =< MsgThreshold ->
                    DelayFactor;
                {Msgs, DelayFactor, _Default} ->
                    ((Msgs div MsgThreshold) + 1 ) * DelayFactor
            end)).

-define(MIN_MASTERS_FILTER(QArgs, F1, F2),
            (case rabbit_misc:table_lookup(QArgs, <<"x-queue-master-locator">>) of
                {_, <<"min-masters">>} -> F1();
                _ -> F2()
             end)).