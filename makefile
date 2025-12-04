# Makefile at project root
PACKAGES := python word-count-sc
POD := custom-artifacts-pod
DEST := /custom-artifacts

# .PHONY tells Make that `build-all` is a command name, not a file name
.PHONY: build-all
build-all:
	@for pkg in $(PACKAGES); do \
		echo "Building $$pkg"; \
		python -m build $$pkg; \
	done

.PHONY: upload-all
upload-all:
	kubectl cp word-count/build/libs/word-count.jar $(POD):$(DEST)/word-count.jar
	kubectl cp python/dist/my_lib-0.1.0-py3-none-any.whl $(POD):$(DEST)/my_lib-0.1.0-py3-none-any.whl
	kubectl cp word-count-sc/dist/wordcount_sc-0.1.0-py3-none-any.whl $(POD):$(DEST)/wordcount_sc-0.1.0-py3-none-any.whl
	kubectl cp word-count-sc/src/python/pyspark_driver/driver.py $(POD):$(DEST)/driver.py
	docker exec -it prod-test-worker2 ls -al /tmp/custom-jars2

.PHONY: build-upload-all
build-upload-all: build-all upload-all

.PHONY: clean
clean:
	rm -rf */dist */build */*.egg-info */*/*.egg-info
