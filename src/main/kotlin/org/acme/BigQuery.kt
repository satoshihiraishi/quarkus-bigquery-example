package org.acme

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableResult
import java.util.UUID
import jakarta.inject.Inject
import jakarta.ws.rs.GET
import jakarta.ws.rs.Path
import jakarta.ws.rs.Produces
import jakarta.ws.rs.core.MediaType

@Path("/bigquery")
class BigQueryResource {
    @Inject
    lateinit var bigquery: BigQuery // Inject BigQuery

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    fun bigquery(): String {
        val queryConfig = QueryJobConfiguration.newBuilder(
            "SELECT CONCAT('https://stackoverflow.com/questions/', CAST(id as STRING)) as url, view_count " +
            "FROM `bigquery-public-data.stackoverflow.posts_questions` " +
            "WHERE tags like '%google-bigquery%' ORDER BY favorite_count DESC LIMIT 10"
        ).setUseLegacySql(false).build()

        val jobId = JobId.of(UUID.randomUUID().toString())
        var queryJob: Job? = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build())

        queryJob = queryJob?.waitFor()

        queryJob ?: throw RuntimeException("Job no longer exists")
        queryJob.status.error?.let {
            throw RuntimeException(it.toString())
        }

        val result: TableResult = queryJob.getQueryResults()
        return result.iterateAll().joinToString(separator = "\n") { row ->
            "${row.get("url").getStringValue()} - ${row.get("view_count").getLongValue()}"
        }
    }
}
