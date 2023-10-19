# Entry point for serra command line tool
import sys
import click
import schedule, time

from serra.run import update_package, run_job_safely
from serra.databricks import create_job
from serra.utils import validate_workspace
from serra.config import PACKAGE_PATH
from serra.utils import copy_folder

@click.group()
def main():
    pass

@main.command(name="run")
@click.argument("job_name")
def cli_run_job_from_job_dir(job_name):
    """Run a specific job locally
    """
    validate_workspace()
    run_job_safely(job_name, "local")


@main.command(name="schedule")
@click.argument("job_name")
@click.argument("interval", type=int)
@click.argument("unit")
def cli_schedule_job(job_name, interval, unit):
    def job_scheduler():
        print('Running...')

        validate_workspace()
        run_job_safely(job_name, "local")

    if unit == 'seconds':
        schedule.every(interval).seconds.do(job_scheduler)
    elif unit == 'minutes':
        schedule.every(interval).minutes.do(job_scheduler)
    elif unit == 'daily':
        schedule.every(interval).days.do(job_scheduler)
    elif unit == 'weekly':
        schedule.every(interval).weeks.do(job_scheduler)

    job_scheduler()
    while True:
        schedule.run_pending()
        time.sleep(1)

@main.command(name="deploy")
@click.argument("job_name")
def cli_create_job(job_name):
    """Create a databricks job
    """
    validate_workspace()
    create_job(job_name)

@main.command(name="create")
@click.argument("local_path", type=click.Path(), default="./workspace")
def cli_create(local_path):
    """Create starter workspace folder"""
    source_folder = f"{PACKAGE_PATH}/data/workspace_example"
    copy_folder(source_folder, local_path)

@main.command(name="update_package")
def cli_update_package():
    """Uploads package to aws, and restarts databricks cluster
    """
    update_package()
    
# only for use by databricks cluster
# Did not use click because there were wierd traceback errors
def serra_databricks():
    assert len(sys.argv) == 2
    job_name = sys.argv[1]
    run_job_safely(job_name, 'aws')
    
if __name__ == '__main__':
  main()