package com.veera.listener;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import com.veera.util.FifoLongPolling;
import com.veera.util.LongPolling;

@Service
public class SQSEventListener {
	
	@Autowired
	private LongPolling longPolling;
	
	@Autowired
	private FifoLongPolling fifoLongPolling;

	@EventListener
	public void listenQueueMessages(ApplicationReadyEvent applicationReadyEvent) {
		System.out.println("Application ready event started");
		while (true) {
			try {
				longPolling.longPollQueueMessages();
				fifoLongPolling.longPollQueueMessages();
			} catch (ClassNotFoundException | IOException e) {
				System.err.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}
}
