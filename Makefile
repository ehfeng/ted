.PHONY: watch build

watch:
	@printf "\033]0;waiting...\007"
	@watchexec --restart --exts go,mod,sum -- \
		'printf "\033]0;building...\007"; go install && (printf "\033]0;built!\007"; sleep 2; printf "\033]0;waiting...\007") || printf "\033]0;waiting...\007"'

build:
	@go install