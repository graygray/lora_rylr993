SUMMARY = "Sample ROS 2 Python Publisher Package for lora_rylr993"
HOMEPAGE = "https://github.com/graygray/lora_rylr993"
AUTHOR = "Your Name <user@example.com>"
LICENSE = "Apache-2.0"

# you can use the common Yocto license directory. Once you add a LICENSE file to your git repo,
# you should change this to something like: file://LICENSE;md5=<checksum>
LIC_FILES_CHKSUM = "file://${COMMON_LICENSE_DIR}/Apache-2.0;md5=89aea4e17d99a7cacdbeed46a0096b10"

#ROS_BRANCH ?= "branch=master"
#SRC_URI = "git://org-169115935@github.com/PMX-CTC/C_AMR-G2_FW_App.git;${ROS_BRANCH};protocol=ssh"
#SRCREV = "${@d.getVar('PMX_AMR_G2_ROS2_APP_REV') or '${AUTOREV}'}"
#S = "${WORKDIR}/git/lora_rylr993"

# test repo
ROS_BRANCH ?= "branch=main"
SRC_URI = "git://git@github.com/graygray/lora_rylr993.git;${ROS_BRANCH};protocol=ssh"
SRCREV = "${AUTOREV}"
S = "${WORKDIR}/git"

# Dependencies required to run the ROS 2 node
RDEPENDS:${PN} = " \
    python3-core \
    python3-pyserial \
    rclpy \
    std-msgs \
"

inherit setuptools3

do_install:append() {
    bbwarn "libdir=${libdir}"
    bbwarn "bindir=${bindir}"

    install -d ${D}${libdir}/lora_rylr993
    if [ -f ${D}${bindir}/lora_rylr993_node ]; then
        ln -sf ${bindir}/lora_rylr993_node ${D}${libdir}/lora_rylr993/lora_rylr993_node
    fi

    install -d ${D}${sysconfdir}/udev/rules.d
    install -m 0644 ${S}/90-lora-uart.rules ${D}${sysconfdir}/udev/rules.d/90-lora-uart.rules
}

FILES:${PN} += " \
    ${libdir}/lora_rylr993/lora_rylr993_node \
    ${datadir}/ament_index/resource_index/packages/lora_rylr993 \
    ${datadir}/lora_rylr993/package.xml \
    ${sysconfdir}/udev/rules.d/90-lora-uart.rules \
"
