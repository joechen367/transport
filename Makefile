APP_VERSION=v1.0.6

# PACKAGE_LIST =  broker/mqtt/ broker/rabbitmq/ server/rabbitmq/ server/asynq/
PACKAGE_LIST =

.PHONY: tag
tag:
	git tag -f $(APP_VERSION) && $(foreach item, $(PACKAGE_LIST), git tag -f $(item)$(APP_VERSION) && ) git push --tags --force