# Use the official latest PostgreSQL image.
FROM postgres:latest

# Set the default environment variables for PostgreSQL.
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres
ENV POSTGRES_DB=ohdsi_tutorial

# Set POSTGRES_PATH dynamically by finding where the `psql` binary is located.
# The `RUN` command runs during the build process and exports the path.
RUN POSTGRES_PATH=$(dirname $(which psql)) && \
    echo "export POSTGRES_PATH=$POSTGRES_PATH" >> /etc/profile && \
    echo "POSTGRES_PATH=$POSTGRES_PATH" >> /etc/environment

# Set POSTGRES_PATH as an environment variable for subsequent container runs.
# Using a hard-coded value for the common installation location. This should match the `RUN` command.
ENV POSTGRES_PATH /usr/lib/postgresql/bin

# Expose the default PostgreSQL port.
EXPOSE 5452