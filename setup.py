import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="ScarletNBA",
    version="0.0.3",
    author="Michael Nestel",
    author_email="nestelm@gmail.com",
    description="A small NBA prediction package",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/meirelon/NBA-NHL",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
