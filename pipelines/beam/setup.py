# O Dataflow instala este pacote nos workers via --setup_file.
import setuptools

setuptools.setup(
    name="mobility-beam",
    version="0.1.0",
    packages=setuptools.find_packages(include=["mobility_beam", "mobility_beam.*"]),
    python_requires=">=3.11",
)
