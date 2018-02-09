The docker file in this directory should be invoked using the docker_build.sh script.
The dockerfile makes assumptions about its build context that are then satisfied by the build script.

HOW THE BUILD CONTEXT IS SET
=============================

If the build context in the shell script is . then this is a relatively simple docker file with mostly external dependencies.
In most other cases the build context will be the nearest ancestor directory to the current directory that will provide a build context that
can pull in any of the project components needed to be included in this build.

Examples:

1. If building the docker file for the current project based on the requirements.txt in the package root directory, the build context will be ../..
   which gets you to the root project directory  (polaris-common, polaris-repos) etc that this docker file is currently in.

2. If building a docker file for a project based on requirements file that has cross project depenednecies (for example polaris-repos, which depends
   on polaris-common) the build context will be the lowest common ancestor directory of the two projects, which is reachable as ../../.. from this directory.


Note that in building package dependencies into images, we are building each image so that it explicitly builds all the package dependencies into the image.
We do not inherit packages between images by making an image containing a package a base image for another image that requires the package. While the latter is
probably also a workable solution, we suspect this method is less error prone as each image is self-contained and built with all its dependencies baked in.






