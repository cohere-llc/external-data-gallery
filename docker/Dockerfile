FROM fedora:latest

# Install Python and uv
RUN dnf update -y && \
    dnf install -y \
    uv \
    git

WORKDIR /gallery
COPY . /gallery

RUN uv python install 3.12 && \
    uv sync && \
    uv pip install pytest nbmake

CMD ["uv", "run", "pytest", "--nbmake"]