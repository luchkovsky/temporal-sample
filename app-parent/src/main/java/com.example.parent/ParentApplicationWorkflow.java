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

package com.example.parent;

import static com.example.parent.SampleConstants.TASK_LIST_COUNT;

import com.uber.m3.tally.RootScopeBuilder;
import com.uber.m3.tally.Scope;
import io.temporal.activity.Activity;
import io.temporal.activity.ActivityInterface;
import io.temporal.activity.ActivityMethod;
import io.temporal.activity.ActivityOptions;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.serviceclient.WorkflowServiceStubs;
import io.temporal.serviceclient.WorkflowServiceStubsOptions;
import io.temporal.worker.Worker;
import io.temporal.worker.WorkerFactory;
import io.temporal.worker.WorkerOptions;
import io.temporal.workflow.Async;
import io.temporal.workflow.ChildWorkflowOptions;
import io.temporal.workflow.Promise;
import io.temporal.workflow.Workflow;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/** Demonstrates a child workflow. Requires a local instance of the Cadence server to be running. */
@Slf4j
@Component
public class ParentApplicationWorkflow implements ApplicationRunner {

  @Override
  public void run(ApplicationArguments args) {
    startFactory();
    startClient();
  }

  private void startFactory() {

    Scope scope =
        new RootScopeBuilder()
            .reporter(new CustomCadenceClientStatsReporter())
            .reportEvery(com.uber.m3.util.Duration.ofSeconds(1));

    WorkflowServiceStubsOptions stubsOptions =
        WorkflowServiceStubsOptions.newBuilder().setMetricsScope(scope).build();

    // gRPC stubs wrapper that talks to the local docker instance of temporal service.
    WorkflowServiceStubs service = WorkflowServiceStubs.newInstance(stubsOptions);

    // client that can be used to start and signal workflows
    WorkflowClient client = WorkflowClient.newInstance(service);

    // worker factory that can be used to create workers for specific task lists
    WorkerFactory factory = WorkerFactory.newInstance(client);

    for (int i = 0; i < TASK_LIST_COUNT; i++) {
      String taskList = SampleConstants.getTaskListParent(i);
      WorkerOptions workerOptions = WorkerOptions.newBuilder().build();

      Worker workerParent = factory.newWorker(taskList, workerOptions);

      workerParent.registerWorkflowImplementationTypes(GreetingWorkflowImpl.class);
      workerParent.registerActivitiesImplementations(new ParentActivitiesImpl());
    }

    // Start listening to the workflow and activity task lists.
    factory.start();
  }

  private void startClient() {

    // gRPC stubs wrapper that talks to the local docker instance of temporal service.
    WorkflowServiceStubs service = WorkflowServiceStubs.newInstance();

    // client that can be used to start and signal workflows
    WorkflowClient client = WorkflowClient.newInstance(service);
    // Get a workflow stub using the same task list the worker uses.
    // Execute a workflow waiting for it to complete.
    GreetingWorkflow parentWorkflow;

    while (true) {
      String taskList = SampleConstants.getTaskListParent();
      WorkflowOptions options = WorkflowOptions.newBuilder().setTaskList(taskList).build();

      parentWorkflow = client.newWorkflowStub(GreetingWorkflow.class, options);

      WorkflowClient.start(parentWorkflow::getGreeting, "World");
      try {
        Thread.sleep(10);

      } catch (InterruptedException e) {
        log.error("Error occurred", e);
      }
    }
    // System.exit(0);
  }

  @WorkflowInterface
  public interface GreetingWorkflow {

    @WorkflowMethod
    String getGreeting(String name);
  }

  @WorkflowInterface
  public interface GreetingChild {

    @WorkflowMethod
    String composeGreeting(String greeting, String name);
  }

  @ActivityInterface
  public interface ParentActivities {

    @ActivityMethod
    String composeParentGreeting(int activityIdx);
  }

  static class ParentActivitiesImpl implements ParentActivities {

    @Override
    public String composeParentGreeting(int activityIdx) {
      //      System.out.printf("[%d] Parent activity done\n", activityIdx);

      String result =
          String.format(
              "Finished parent activity: idx: [%d], activity id: [%s], task: [%s]",
              activityIdx, Activity.getTask().getActivityId(), new String(Activity.getTaskToken()));
      System.out.println(result);
      return result;
    }
  }

  /** GreetingWorkflow implementation that calls GreetingsActivities#printIt. */
  public static class GreetingWorkflowImpl implements GreetingWorkflow {

    @Override
    public String getGreeting(String name) {
      String taskList = SampleConstants.getTaskListChild();

      ChildWorkflowOptions options =
          ChildWorkflowOptions.newBuilder().setTaskList(taskList).build();

      // Workflows are stateful. So a new stub must be created for each new child.
      GreetingChild child = Workflow.newChildWorkflowStub(GreetingChild.class, options);

      // This is a blocking call that returns only after the child has completed.
      Promise<String> greeting = Async.function(child::composeGreeting, "Hello", name);

      // Do something else here.
      Promise<Void> result = runParentActivities();
      //      System.out.println(
      //          "Got result in parent: " + String.join(";\n", parentPromises.get()) + "\n");

      return greeting.get(); // blocks waiting for the child to complete.
    }

    private Promise<Void> runParentActivities() {
      List<Promise<?>> parentActivities = new ArrayList<>();
      for (int i = 0; i < 5; i++) {
        String taskList = SampleConstants.getTaskListParent();
        ActivityOptions ao =
            ActivityOptions.newBuilder()
                .setTaskList(taskList)
                .setScheduleToCloseTimeout(Duration.ofSeconds(60))
                .build();

        ParentActivities activity = Workflow.newActivityStub(ParentActivities.class, ao);
        parentActivities.add(Async.function(activity::composeParentGreeting, i));
      }

      return Promise.allOf(parentActivities);
    }
  }
}
