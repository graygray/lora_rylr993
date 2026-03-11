from setuptools import find_packages, setup

package_name = 'lora_rylr993'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools', 'pyserial'],
    zip_safe=True,
    maintainer='Your Name',
    maintainer_email='user@example.com',
    description='Sample ROS 2 Python Node - lora_rylr993',
    license='Apache-2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'lora_rylr993_node = lora_rylr993.lora_rylr993_node:main'
        ],
    },
)
