PROJECT = rabbitmq_queue_master_balancer
PROJECT_DESCRIPTION = Queue Master balancing tool for RabbitMQ
PROJECT_MOD = rabbit_queue_master_balancer_app

define PROJECT_ENV
[
  {operational_priority,     10},
  {preload_queues,           true},
  {sync_delay_timeout,       10000},
  {sync_verification_factor, 300},
  {policy_transition_delay,  100}
     ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit amqp_client
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
