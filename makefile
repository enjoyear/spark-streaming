# Makefile at project root
PACKAGES := python word-count-sc

.PHONY: build-all
build-all:
	@for pkg in $(PACKAGES); do \
		echo "Building $$pkg"; \
		python -m build $$pkg; \
	done

.PHONY: clean
clean:
	rm -rf */dist */build */*.egg-info */*/*.egg-info