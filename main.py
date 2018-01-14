from dataengineeringutils import glue
if __name__ == "__main__":
    glue.delete_job("flights_a")
    job_path = "s3://robintest-gluenotebooks3files/glue_jobs/job_test/"
    glue.run_glue_job_from_local_folder_template("glue_jobs/job_test/", job_path, name="flights_a", role="robintest-gluerole", AllocatedCapacity = 5)
    
    glue.delete_job("flights_b")
    job_path = "s3://robintest-gluenotebooks3files/glue_jobs/job_test2/"
    glue.run_glue_job_from_local_folder_template("glue_jobs/job_test2/", job_path, name="flights_b", role="robintest-gluerole", AllocatedCapacity = 5)