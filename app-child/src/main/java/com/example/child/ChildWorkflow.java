/*
 *  Copyright 2012-2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Modifications copyright (C) 2017 Uber Technologies, Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"). You may not
 *  use this file except in compliance with the License. A copy of the License is
 *  located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed on
 *  an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.example.child;

import static com.example.parent.SampleConstants.ACTIVITIES_COUNT;
import static com.example.parent.SampleConstants.TASK_LIST_COUNT;
import static com.example.parent.SampleConstants.getTaskListChild;

import com.example.parent.SampleConstants;
import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import com.uber.m3.util.Duration;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerFactoryOptions;
import io.temporal.worker.WorkerOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class ChildWorkflow implements ApplicationRunner {

  @Override
  public void run(ApplicationArguments args) {

    startFactory();
  }

  private void startFactory() {

    Scope scope =
        new RootScopeBuilder()
            .reporter(new CustomCadenceClientStatsReporter())
            .reportEvery(com.uber.m3.util.Duration.ofSeconds(1));

    WorkerFactoryOptions factoryOptions = WorkerFactoryOptions.newBuilder().build();

    WorkflowServiceStubsOptions stubsOptions =
        WorkflowServiceStubsOptions.newBuilder().setMetricsScope(scope).build();
    // gRPC stubs wrapper that talks to the local docker instance of temporal service.
    WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(stubsOptions);
    // client that can be used to start and signal workflows
    WorkflowClient client = WorkflowClient.newInstance(service);

    // worker factory that can be used to create workers for specific task lists
    WorkerFactory factory = WorkerFactory.newInstance(client);

    for (int i = 0; i < TASK_LIST_COUNT; i++) {
      String taskList = SampleConstants.getTaskListChild(i);
      WorkerOptions workerOptions = WorkerOptions.newBuilder().build();

      Worker workerChild = factory.newWorker(taskList, workerOptions);

      workerChild.registerWorkflowImplementationTypes(GreetingChildImpl.class);
      workerChild.registerActivitiesImplementations(new GreetingActivitiesImpl());
    }

    // Start listening to the workflow and activity task lists.
    factory.start();
  }

  /** The child workflow interface. */
  @WorkflowInterface
  public interface GreetingChild {

    @WorkflowMethod
    String composeGreeting(String greeting, String name);
  }

  @ActivityInterface
  public interface GreetingActivities {

    @ActivityMethod
    String composeGreeting(String greeting, String name);
  }

  /**
   * The child workflow implementation. A workflow implementation must always be public for the
   * Cadence library to be able to create instances.
   */
  public static class GreetingChildImpl implements GreetingChild {

    public String composeGreeting(String greeting, String name) {
      long startSW = System.nanoTime();
      List<Promise<?>> activities = new ArrayList<>();

      for (int i = 0; i < ACTIVITIES_COUNT; i++) {
        String taskList = getTaskListChild();
        ActivityOptions ao =
            ActivityOptions.newBuilder()
                .setTaskList(taskList)
                .setScheduleToCloseTimeout(java.time.Duration.ofSeconds(60))
                .build();

        GreetingActivities activity = Workflow.newActivityStub(GreetingActivities.class, ao);
        activities.add(Async.function(activity::composeGreeting, greeting + i, name));
      }

      Promise<Void> greetingActivities = Promise.allOf(activities);
      String result = greetingActivities.get() + " " + name + "!";
      System.out.println(
          "Duration of childwf - " + Duration.between(startSW, System.nanoTime()).getSeconds());

      return result;
    }
  }

  static class GreetingActivitiesImpl implements GreetingActivities {
    @Override
    public String composeGreeting(String greeting, String name) {
      byte[] taskToken = Activity.getTaskToken();
      sendRestRequest(taskToken);
      Activity.doNotCompleteOnReturn();

      return "Activity paused";
    }
  }

  private static void sendRestRequest(byte[] taskToken) {
    try {
      URL url = new URL("http://127.0.0.1:8090/api/cadence/async");
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setDoOutput(true);
      connection.setInstanceFollowRedirects(false);
      connection.setRequestMethod("PUT");
      connection.setRequestProperty("Content-Type", "application/octet-stream");

      OutputStream os = connection.getOutputStream();
      os.write(taskToken);
      os.flush();

      connection.getResponseCode();
      connection.disconnect();

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
