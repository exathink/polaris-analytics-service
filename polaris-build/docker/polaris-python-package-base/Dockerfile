ARG BUILDER_IMAGE
ARG RELEASE_IMAGE


FROM ${BUILDER_IMAGE}
# This Dockerfile builds a "package image" for a python package which is a docker image containing
# and installed version of the package and all its dependencies.

# The Dockerfile expects that the context of the build passed in the build command is the root of the repository for which the package image is
# being built. It also expects that the polaris-build repository is mounted as a submodule of this repository
# and so will be available in the build context as /project/polaris-build

# First we separately build the dependencies of the package using requirements.txt. This will allow us to
# take advantage of image caching as we download and build the dependencies. This cache will be valid as long
# as neither dependencies or requirements.txt change.
COPY  *requirements.txt \
      polaris-build/system-pip-constraints.txt \
      /tmp/build/

RUN  touch /tmp/build/all-requirements.txt \
     && cat /tmp/build/all-requirements.txt | grep -v polaris > /tmp/build/dependencies.txt || true \
     && pip install -r /tmp/build/dependencies.txt -c /tmp/build/system-pip-constraints.txt \
     && rm -rf /tmp/build

# Now we build the package itself.
# Install.sh must be in the root directory of the repository being built. This invokes polaris-build/scripts/build_python_packages.sh which
# recursively builds all the submdodules before calling setup.py of the top level repository to install this package
COPY . /project/
WORKDIR /project
RUN mkdir -p /project/wheels && \
    ./install.sh --dependencies-only


RUN pip freeze | grep -v polaris > /project/requirements.txt || true \
    && pip freeze | grep polaris > /project/polaris-requirements.txt || true

FROM ${RELEASE_IMAGE}
COPY --from=0 /project/wheels /tmp/build/wheels/
COPY --from=0 /project/requirements.txt /project/polaris-requirements.txt /tmp/build/

RUN cat /tmp/build/*requirements.txt | xargs -I '?' pip install --no-index --find-links /tmp/build/wheels ? \
    && rm -rf /tmp/build









