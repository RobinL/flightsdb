from dataengineeringutils import glue
if __name__ == "__main__":
    glue.delete_job("flights")
    jobs_path = "s3://robintest-gluenotebooks3files/glue_jobs/"
    glue.run_glue_job_from_local_folder_template("glue_jobs/job_test/", jobs_path, name="flights", role="robintest-gluerole", AllocatedCapacity = 5)