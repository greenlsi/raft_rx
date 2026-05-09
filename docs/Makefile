prefix=raft-rx-

pdf: $(prefix)specs.pdf $(prefix)user-python.pdf $(prefix)user-c.pdf

PANDOC_FLAGS = \
	--from markdown \
	--template eisvogel \
	-M date="$(shell date +%Y-%m-%d)" \
	-M titlepage=true \
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

$(prefix)specs.pdf: specs.md requirements.md design.md tasks.md
	@echo "Generating $@..."
	@pandoc $^ -o $@ $(PANDOC_FLAGS)

$(prefix)user-python.pdf: user-python.md
	@echo "Generating $@..."
	@pandoc $^ -o $@ $(PANDOC_FLAGS) \
		-M title="raft-rx — User guide: Python" \
		-M subtitle="FSM-based Raft implementation over rxnet"

$(prefix)user-c.pdf: user-c.md
	@echo "Generating $@..."
	@pandoc $^ -o $@ $(PANDOC_FLAGS) \
		-M title="raft-rx — User guide: C" \
		-M subtitle="FSM-based Raft implementation over rxnet"

clean:
	rm -f $(prefix)specs.pdf $(prefix)user-python.pdf $(prefix)user-c.pdf
