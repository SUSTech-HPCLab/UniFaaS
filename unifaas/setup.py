from setuptools import setup, find_packages

with open('./unifaas/version.py') as f:
    exec(f.read())

with open('requirements.txt') as f:
    install_requires = f.readlines()

setup(
    name='unifaas',
    version=VERSION,
    description='Simple data dependent workflows in Python',
    long_description='Simple parallel workflows system for Python',
    url='https://github.com/SUSTech-HPCLab/UniFaaS',
    author='SUSTech HPCLab',
    author_email='12232396@mail.sustech.edu.cn',
    license='Apache 2.0',
    include_package_data=True,
    packages=find_packages(),
    python_requires=">=3.6.0",
    install_requires=install_requires,
    classifiers=[
        # Maturity
        'Development Status :: 3 - Alpha',
        # Intended audience
        'Intended Audience :: Developers',
        # Licence, must match with licence above
        'License :: OSI Approved :: Apache Software License',
        # Python versions supported
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    keywords=['Workflows', 'Scientific computing'],
)