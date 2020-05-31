package com.example.child;

import com.example.parent.SampleConstants;
import com.uber.cadence.client.ActivityCompletionClient;
import com.uber.cadence.client.ActivityCompletionException;
import com.uber.cadence.client.WorkflowClient;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@Slf4j
public class ChildRestController {

  private ActivityCompletionClient completionClient;

  @PostConstruct
  void init() {
    WorkflowClient workflowClient =
        WorkflowClient.newInstance("127.0.0.1", 7933, SampleConstants.DOMAIN);
    completionClient = workflowClient.newActivityCompletionClient();
  }

  @PutMapping(value = "/result")
  @ResponseBody
  public ResponseEntity<byte[]> handle(@RequestBody byte[] taskToken) {
    if (completionClient != null) {
      String response = "This is the response";
      try {
        completionClient.complete(taskToken, "THIS IS THE END!");
        log.info("Complete activity!");
      } catch (ActivityCompletionException e) {
        log.error("Can't complete activity!", e);
      }
      return ResponseEntity.ok().body(response.getBytes());
    }

    return ResponseEntity.notFound().build();
  }

  @GetMapping("/test")
  public String check() {
    return "REST API is available";
  }
}
