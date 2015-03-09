package Test;
import java.io.File;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class AWSObject {

	private AWSCredentials credentials; 
	private AmazonS3 s3;


	//************Constructors and Getters****************
	/**
	 * Get your credentials from the "credentials" file inside you .aws folder
	 */
	public AWSObject(){
		credentials = new ProfileCredentialsProvider().getCredentials();

	}

	/**
	 * Get your credentials by hard-coded code
	 */
	public AWSObject(String accesKey, String secretKey)
	{
		credentials = new BasicAWSCredentials(accesKey, secretKey);
	}

	public AWSCredentials getCredentials(){
		return credentials;
	}


	

	//**************S3 METHODS******************
	/**
	 * initialize S3 services	
	 */
	public void initS3(){
		s3 = new AmazonS3Client(credentials);
	}

	/**
	 * @return S3 url of the uploaded file
	 */
	public String S3UploadFile(String bucketName, String key, File inputFile){
		//Open connection with the S3 client
		s3.createBucket(bucketName);

		//Upload the input file to the bucket
		s3.putObject(new PutObjectRequest(bucketName, key, inputFile));

		//Return the url of the uploaded file
		return "https://s3.amazonaws.com/"+bucketName+"/"+key;

	}

	/**
	 * @return object at the bucket and with the key specified
	 */
	public S3Object S3DownloadFile(String bucketName, String key){
		return s3.getObject(new GetObjectRequest(bucketName, key));
	}

	/**
	 * Delete object from the bucket
	 */
	public void S3DeleteObject(String bucketName, String key){
		s3.deleteObject(new DeleteObjectRequest(bucketName, key));
	}


	/**
	 * A bucket must be completely empty before it can be deleted
	 * @param bucketName to delete
	 */
	public void S3DeleteBucket(String bucketName) {
		s3.deleteBucket(bucketName);
	}

	/**
	 * Deletes a bucket and all the files inside
	 */
	public void S3ForceDeleteBucket(String bucketName){
		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest().withBucketName(bucketName));
		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
			S3DeleteObject(bucketName, objectSummary.getKey());
		}

		S3DeleteBucket(bucketName);
	}


	
}
