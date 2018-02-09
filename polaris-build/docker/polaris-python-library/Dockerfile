ARG BUILDER_IMAGE
FROM ${BUILDER_IMAGE}
# This Dockerfile builds a "library image" for a python package which is a docker image containing
# only the external dependencies of the package, but not any of the polaris packages that it depends on.
# This Dockerfile is very closely related to the Dockerfile in 'polaris-python-package'. The build steps in this
# Dockerfile are actually a prefix of the build steps in building that library.

# The Dockerfile expects that the context of the build passed in the build command is the root of the repository for which the package image is
# being built. It also expects that the polaris-build repository is mounted as a submodule of this repository
# and so will be available in the build context as /project/polaris-build

# First we separately build the dependencies of the package using requirements.txt. This will allow us to
# take advantage of image caching as we download and build the dependencies. This cache will be valid as long
# as neither dependencies or requirements.txt change.
COPY  requirements.txt \
      polaris-build/system-pip-constraints.txt \
      /tmp/build/

RUN pip install -r /tmp/build/requirements.txt -c /tmp/build/system-pip-constraints.txt \
    && rm -rf /tmp/build

# When tests are run we will mount the root of the repository as the
# as a volume. The polaris pacakges are accessed directly from source as opposed to
# installing them. This is the main difference between the package image and library image.
# This image is intended to be used in development scenarios where we can make changes to sources and
# run tests without rebuilding a new image as long as the external dependencies have not changed.
VOLUME /project
WORKDIR /project
