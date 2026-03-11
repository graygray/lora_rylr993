SUMMARY = "Sample ROS 2 Python Publisher Package for lora_rylr993"
HOMEPAGE = "https://github.com/graygray/lora_rylr993"
AUTHOR = "Your Name <user@example.com>"
LICENSE = "Apache-2.0"
# Usually yocto requires a license file. If you don't have one in your repo yet, 
# you can use the common Yocto license directory. Once you add a LICENSE file to your git repo,
# you should change this to something like: file://LICENSE;md5=<checksum>
LIC_FILES_CHKSUM = "file://${COMMON_LICENSE_DIR}/Apache-2.0;md5=89aea4e17d99a7cacdbeed46a0096b10"

ROS_BRANCH ?= "branch=main"
# Yocto git fetcher with SSH protocol
SRC_URI = "git://git@github.com/graygray/lora_rylr993.git;${ROS_BRANCH};protocol=ssh"
SRCREV = "${AUTOREV}"

# The source directory after unpacking
S = "${WORKDIR}/git"

# Dependencies required to run the ROS 2 node
RDEPENDS:${PN} = " \
    python3-core \
    python3-pyserial \
    rclpy \
    std-msgs \
"

inherit setuptools3

# Instruct Yocto to package the additional files we installed via setup.py
# Ensure ros2 run can find the Python console script under lib/<pkg>.
do_install:append() {
    bbwarn "libdir=${libdir}"
    bbwarn "bindir=${bindir}"

    install -d ${D}${libdir}/lora_rylr993
    if [ -f ${D}${bindir}/lora_rylr993_node ]; then
        ln -sf ${bindir}/lora_rylr993_node ${D}${libdir}/lora_rylr993/lora_rylr993_node
    fi
}

# Instruct Yocto to package the additional files we installed via setup.py and do_install.
FILES:${PN} += " \
    ${libdir}/lora_rylr993/lora_rylr993_node \
    ${datadir}/ament_index/resource_index/packages/lora_rylr993 \
    ${datadir}/lora_rylr993/package.xml \
"
