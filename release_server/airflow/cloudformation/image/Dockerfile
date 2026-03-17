# ============================================
# Stage 1: base-image
# ============================================
FROM alpine:3.19 AS base-image
RUN echo "This is the base image"
RUN apk add --no-cache curl

# ============================================
# Stage 2: websched (depends on base-image)
# ============================================
FROM base-image AS websched
RUN echo "This is websched image"
LABEL component="websched"

# ============================================
# Stage 3: worker (depends on base-image)
# ============================================
FROM base-image AS worker
RUN echo "This is worker image"
LABEL component="worker"
