pdf: user-guide.pdf

PANDOC_FLAGS = \
	--from markdown \
	--template eisvogel \
	-M date="$(shell date +%Y-%m-%d)" \
	-M titlepage=true \
	-M titlepage-color="006EB7" \
	-M titlepage-text-color="FFFFFF" \
	-M titlepage-rule-color="FFFFFF" \
	-M titlepage-rule-height=2 \
	-M listings=true \
	-M book=true \
	--syntax-highlighting idiomatic \
	--toc \
	--toc-depth=3 \
	--lof \
	-V caption-justification=centering \
	-V documentclass=report \
	--top-level-division=chapter \
	--filter pandoc-crossref \
	--filter mermaid-filter \
	--pdf-engine=xelatex

user-guide.pdf: user-guide.md
	@echo "Generating $@..."
	@pandoc $^ -o $@ $(PANDOC_FLAGS) \
		-M title="raft_rx - C User Guide" \
		-M subtitle="Verifiable Raft consensus for embedded systems"

clean:
	rm -f user-guide.pdf
