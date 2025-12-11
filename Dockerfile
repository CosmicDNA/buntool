# Stage 1: The Builder
# This stage compiles the Python code into a Nuitka wheel.
FROM python:3.11-alpine AS builder

# Install build-time dependencies for Nuitka (C compiler etc.)
RUN apk add --no-cache build-base

WORKDIR /app

# Copy project files
COPY pyproject.toml README.md ./
COPY buntool ./buntool
COPY licenses ./licenses

# Upgrade pip and install the build tool
RUN pip install --upgrade pip
RUN pip install build

# Build the Nuitka-compiled wheel.
# The `build` command will read pyproject.toml, install Nuitka,
# and use it as the build backend.
RUN python -m build --wheel


# Stage 2: The Runner
# This stage creates the final, lightweight image to run the application.
FROM python:3.11-alpine

# Install runtime system dependencies.
# - qpdf is required by pikepdf.
RUN apk add --no-cache qpdf

# Create a non-root user and group for security
RUN addgroup -S buntool_group && adduser -S buntool_user -G buntool_group

WORKDIR /app

# Create directories for logs, temp files, and output bundles,
# and set ownership to the non-root user.
RUN mkdir -p tempfiles logs bundles && \
    chown -R buntool_user:buntool_group /app

# Copy the compiled wheel from the builder stage
COPY --from=builder /app/dist/*.whl .

# Install the wheel. This will also install runtime dependencies from pyproject.toml.
RUN pip install --no-cache-dir *.whl && \
    # Clean up the wheel file after installation
    rm *.whl

# Switch to the non-root user
USER buntool_user

# Expose the port the application runs on
EXPOSE 7001

# Define the command to run the application
CMD ["buntool"]
