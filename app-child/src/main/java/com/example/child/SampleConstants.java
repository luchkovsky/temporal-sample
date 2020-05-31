package com.example.parent;

import java.util.Random;
import java.util.StringJoiner;

public class SampleConstants {

  public static final int POLL_THREAD_COUNT = 10;
  public static final int TASK_LIST_COUNT = 1;
  public static final int ACTIVITIES_COUNT = 10;

  public static final String DOMAIN = "sample";

  private static final Random random = new Random();

  private static final String TASK_LIST_PARENT = "HelloParent";
  private static final String TASK_LIST_CHILD = "HelloChild";

  public static String getTaskListParent(int i) {
    return new StringJoiner("_").add(TASK_LIST_PARENT).add(String.valueOf(i)).toString();
  }

  public static String getTaskListParent() {
    return getTaskListParent(random.nextInt(TASK_LIST_COUNT));
  }

  public static String getTaskListChild(int i) {
    return new StringJoiner("_").add(TASK_LIST_CHILD).add(String.valueOf(i)).toString();
  }

  public static String getTaskListChild() {
    return getTaskListChild(random.nextInt(TASK_LIST_COUNT));
  }
}
