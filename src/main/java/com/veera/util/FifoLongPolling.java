package com.veera.util;

import java.io.IOException;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.AmazonSQSException;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;
import com.amazonaws.services.sqs.model.SetQueueAttributesRequest;
import com.veera.data.Employee;

@Service
public class FifoLongPolling {
	
	@Value("${FIFO_EMP_QUEUE_URL}")
	private String fifoQueueUrl;
	
	@Value("${AWS_ACCESS_KEY}")
	private String accessKey;
	
	@Value("${AWS_SECRET_KEY}")
	private String secretKey;
	
	@Value("${FIFO_EMP_QUEUE_NAME}")
	private String fifoQueueName;

//	@EventListener
//	public void listenQueueMessages(ApplicationReadyEvent applicationReadyEvent) {
//		System.out.println("Application ready event started");
//		while (true) {
//			try {
//				longPollQueueMessages();
//			} catch (ClassNotFoundException | IOException e) {
//				System.err.println(e.getMessage());
//				e.printStackTrace();
//			}
//		}
//	}

	public void longPollQueueMessages() throws ClassNotFoundException, IOException {
		// final AmazonSQS sqs = AmazonSQSClientBuilder.defaultClient();

		BasicAWSCredentials awsCreds = new BasicAWSCredentials(accessKey,secretKey);
		AmazonSQS amazonSQS = AmazonSQSClientBuilder.standard()
				.withCredentials(new AWSStaticCredentialsProvider(awsCreds)).build();

		// Enable long polling when creating a queue
		CreateQueueRequest create_request = new CreateQueueRequest().withQueueName(fifoQueueName)
				.addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");

		try {
			amazonSQS.createQueue(create_request);
		} catch (AmazonSQSException e) {
			if (!e.getErrorCode().equals("QueueAlreadyExists")) {
				throw e;
			}
		}

		// Enable long polling on an existing queue
		SetQueueAttributesRequest set_attrs_request = new SetQueueAttributesRequest().withQueueUrl(fifoQueueUrl)
				.addAttributesEntry("ReceiveMessageWaitTimeSeconds", "20");
		amazonSQS.setQueueAttributes(set_attrs_request);

		// Enable long polling on a message receipt
		ReceiveMessageRequest receive_request = new ReceiveMessageRequest().withQueueUrl(fifoQueueUrl)
				.withWaitTimeSeconds(20);
		receive_request.setMaxNumberOfMessages(10);
		ReceiveMessageResult msgResult = amazonSQS.receiveMessage(receive_request);
		List<Message> receiveMsgs = msgResult.getMessages();
		System.out.println("....Fifo polling messages size...."+receiveMsgs.size());
		for (Message resMsg : receiveMsgs) {
			String responseMesg = resMsg.getBody();
			Object desObj = Utils.deserializeFromBase64(responseMesg);
			Employee emp = (Employee) desObj;
			System.out.println("Fifo Response Body : " + emp.toString());
			deleteMsgs(amazonSQS, fifoQueueUrl, resMsg.getReceiptHandle());
		}
	}

	private void deleteMsgs(AmazonSQS amazonSQS, String empQueueUrl, String receiptHandle) {
		amazonSQS.deleteMessage(empQueueUrl, receiptHandle);
	}
}
