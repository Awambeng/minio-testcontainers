## Integration Test Configuration with Podman

This documentation provides an overview of how the integration tests in the project are configured to use Podman as the container runtime. Podman is used to spin up test containers for running the integration tests, providing an isolated and consistent environment for testing.

### Prerequisites


Before configuring the integration tests with Podman, ensure that the following prerequisites are met:


1. Ubuntu Server: The integration test configuration described here is specific to an Ubuntu Server environment. Make sure you have an Ubuntu Server available to run the tests.

2. Docker Registries: The integration tests rely on Docker images for the test containers. Ensure that the necessary Docker registries are accessible to pull the required images. By default, the configuration uses the `docker.io` registry.

### Configuration Steps

Follow the steps below to configure the integration tests to use Podman for running test containers:

1. Set the Environmental Variables:


   ```yaml
   # Set the environmental variables
   ENV DOCKER_HOST=unix://${XDG_RUNTIME_DIR}/podman/podman.sock
   ENV TESTCONTAINERS_RYUK_DISABLED=true
   ```

   These environmental variables ensure that Podman is used as the container runtime and disable the Ryuk container, which is not required for the integration tests.

2. Configure Podman in the Workflow:


   ```yaml
   - name: Set up Podman
     run: |
       sudo apt-get update
       sudo apt-get install -y podman
   ```

   This step installs Podman on the Ubuntu Server.

3. Configure Podman for Integration Tests:


   ```yaml
   - name: Configure Podman for integration tests
     run: |
       echo "[registries.search]" | sudo tee -a /etc/containers/registries.conf
       echo "registries=['docker.io']" | sudo tee -a /etc/containers/registries.conf
   ```

   These steps update the `registries.conf` file for Podman, specifying the Docker registry (`docker.io`) from which the required Docker images can be pulled.

4. Build and Run Integration Tests:

   Finally, configure the workflow to build and run the integration tests using Maven. For example:

   ```yaml
   - name: Build and test with Maven (Integration Tests)
     run: mvn test -Pintegration-tests
   ```

   This step executes the Maven command to run the integration tests with the specified Maven profile (`-Pintegration-tests`).

### Conclusion

By following the above configuration steps, the integration tests in the project are set up to use Podman as the container runtime. This allows for consistent and isolated testing of the application within test containers. Ensure that the necessary Docker images are available in the specified Docker registry (`docker.io`) for the integration tests to run successfully.

