# Init-container image for the spike: jk-agent + pebble (docs/design.md, Pod and object layout: "copies agent, hook tools and pebble").
FROM scratch
COPY jk-agent /jk-agent
COPY pebble /pebble
ENTRYPOINT ["/jk-agent"]
