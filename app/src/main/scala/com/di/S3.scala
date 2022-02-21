package com.di

import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.di.domain.{MobileAppClickStreamRow, UserPurchasesRow}
import com.typesafe.config.Config
import org.apache.commons.io.IOUtils
import org.apache.log4j.Logger

import java.io.{BufferedReader, ByteArrayInputStream, FileOutputStream, InputStreamReader}
import java.nio.file.{Files, Paths}
import java.util.stream
import java.util.zip.GZIPInputStream
import scala.util.{Failure, Success, Try}

class S3(config: Config, logger: Logger) {
  private val accessKey: String = config.getString("s3.access-key")
  private val secretKey: String = config.getString("s3.secret-key")
  private val region: String = config.getString("s3.region")
  private val bucketName: String = config.getString("s3.bucket-name")

  private val creds = new BasicAWSCredentials(accessKey, secretKey)
  private val s3Client = AmazonS3ClientBuilder
    .standard()
    .withCredentials(new AWSStaticCredentialsProvider(creds))
    .withRegion(region)
    .build()

  private def decompress(compressed: Array[Byte]): stream.Stream[String] = {
    Try (
      new BufferedReader(
        new InputStreamReader(
          new GZIPInputStream(
            new ByteArrayInputStream(compressed))))
    ) match {
      case Success(inputStream) => inputStream.lines()
      case Failure(ex) =>
        logger.error(s"Decompression failed due to $ex")
        throw DataDownloadException(ex.getLocalizedMessage)
    }
  }

  private def fileExists(fileName: String): Boolean =
    Files.exists(Paths.get(s"data/${fileName}"))

  //todo: exceptions
  private def saveToFile(fileName: String, decompressedStream: stream.Stream[String]): Unit = {
    val unGzippedFileName = fileName.replace(".gz", "")

    if (fileExists(unGzippedFileName)) logger.warn(s"File $unGzippedFileName already exists")
    else {
      val file = new FileOutputStream("data/" + unGzippedFileName)

      decompressedStream.forEach { line =>
        file.write(line.getBytes())
        file.write("\n".getBytes())
      }
      file.close()

    }
  }

  private def downloadDataFromSource(source: String): Unit = {
    Files.createDirectory(Paths.get("data/" + source))
    val rangeOfFiles = (1 to 20)
    val dataSourceWithPrefix = source + "/" + source

    for (index <- rangeOfFiles) {
      val key = s"${dataSourceWithPrefix}_$index.csv.gz"
      logger.info(s"Downloading $key")
      Try(s3Client.getObject(bucketName, key)) match {
        case Success(obj) =>
          val bytes = IOUtils.toByteArray(obj.getObjectContent)
          val decompressedStream = decompress(bytes)
          saveToFile(key, decompressedStream)

        case Failure(_) =>
          logger.warn(s"No such key: $key")
      }
    }
  }

  def downloadData(isRequired: Boolean): Unit = {
    if (isRequired) Try {
      Files.createDirectory(Paths.get("data"))
      downloadDataFromSource(MobileAppClickStreamRow.prefix)
      downloadDataFromSource(UserPurchasesRow.prefix)
    } match {
      case Success(_) => ()
      case Failure(ex) => throw DataDownloadException(ex.getLocalizedMessage)
    }
    else logger.info("Data from s3 will not be downloaded")
  }
}

case class DataDownloadException(reason: String) extends Exception(reason)