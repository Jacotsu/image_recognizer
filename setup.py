from setuptools import setup

setup(
    name='image_recognizer',
    url="https://github.com/Jacotsu/image_recognizer",
    author="Raffaele Di Campli",
    author_email="dcdrj.pub@gmail.com",
    license='AGPLv3+',
    install_requires=[
        "image_match",
        "numpy"
    ],
    python_requires='>=3.7',
    packages=['image_recognizer'],
    entry_points={
        'console_scripts': [
            'image_recognizer=image_recognizer.find_duplicates:main',
        ]
    }
)
