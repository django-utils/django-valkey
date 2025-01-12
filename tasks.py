import pathlib
import shutil

from invoke import task


@task
def devenv(c):
    clean(c)
    cmd = "docker compose --profile all up -d"
    c.run(cmd)


@task
def clean(c):
    if pathlib.Path("build").is_dir():
        shutil.rmtree("build")
    if pathlib.Path("dist").is_dir():
        shutil.rmtree("dist")

    c.run("docker compose --profile all rm -s -f")
