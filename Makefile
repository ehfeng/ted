.PHONY: watch build debug release

watch:
	@printf "\033]0;waiting...\007"
	@watchexec --restart --exts go,mod,sum -- \
		'printf "\033]0;building...\007"; go install -tags debug && ted completion zsh > /usr/local/share/zsh/site-functions/_ted && (printf "\033]0;built!\007"; sleep 2; printf "\033]0;waiting...\007") || printf "\033]0;waiting...\007"'

build: debug

debug:
	@go install -tags debug

release:
	@go install