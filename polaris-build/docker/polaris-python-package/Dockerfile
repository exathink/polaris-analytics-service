ARG PACKAGE_BASE_IMAGE
FROM ${PACKAGE_BASE_IMAGE}

COPY . /project/
WORKDIR /project

RUN ./install.sh \
    && rm -rf /project/*


# Delete the project source directories from the image and copy back only the directories
# we want to keep.

# Copy test files back so we can run tests against the image
COPY conftest.py pytest.ini /project/
COPY test /project/test/

# Install any existing executables to /usr/local/bin
COPY polaris-build/ignore-missing bin/* /usr/local/bin/
