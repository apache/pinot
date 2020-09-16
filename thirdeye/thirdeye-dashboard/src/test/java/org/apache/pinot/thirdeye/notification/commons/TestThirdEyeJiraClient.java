package org.apache.pinot.thirdeye.notification.commons;

import com.atlassian.jira.rest.client.api.domain.CimProject;
import com.atlassian.jira.rest.client.api.domain.input.ComplexIssueInputFieldValue;
import com.atlassian.jira.rest.client.api.domain.input.IssueInput;
import com.atlassian.jira.rest.client.internal.json.CreateIssueMetadataJsonParser;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class TestThirdEyeJiraClient {

  @Test
  public void testBuildIssue() throws JSONException, IOException {
    JiraEntity jiraEntity = new JiraEntity("test_project", 19L, "test_summary");
    jiraEntity.setAssignee("test_assignee");
    jiraEntity.setLabels(Arrays.asList("test_1", "test_2"));
    jiraEntity.setDescription("test_description");
    Map<String, Object> custom = new HashMap<>();
    custom.put("test1", "value1");
    custom.put("test2", "value2");
    jiraEntity.setCustomFieldsMap(custom);

    JiraConfiguration jiraConfig = new JiraConfiguration();
    jiraConfig.setJiraHost("host");
    jiraConfig.setJiraUser("user");
    jiraConfig.setJiraPassword("passwd");

    // construct mock metadata from sample api response in "jira_create_schema.json"
    String jsonString = IOUtils.toString(this.getClass().getResourceAsStream("jira_create_schema.json"));
    JSONObject jsonObj = new JSONObject(jsonString);
    CreateIssueMetadataJsonParser parser = new CreateIssueMetadataJsonParser();
    Iterable<CimProject> CimProjectIt = parser.parse(jsonObj);

    ThirdEyeJiraClient teJiraClient = new ThirdEyeJiraClient(jiraConfig);
    ThirdEyeJiraClient teJiraClientSpy = Mockito.spy(teJiraClient);
    Mockito.doReturn(CimProjectIt).when(teJiraClientSpy).getProjectMetadata(any(JiraEntity.class));
    IssueInput issueInput = teJiraClientSpy.buildIssue(jiraEntity);

    // Assert if all the parameters are set
    Assert.assertEquals(((ComplexIssueInputFieldValue) issueInput.getField("assignee").getValue())
        .getValuesMap().values().toString(), "[test_assignee]");
    Assert.assertEquals(((ComplexIssueInputFieldValue) issueInput.getField("project").getValue())
        .getValuesMap().values().toString(), "[test_project]");
    Assert.assertEquals(((List) issueInput.getField("labels").getValue()), Arrays.asList("test_1", "test_2"));
    Assert.assertEquals(issueInput.getField("summary").getValue(), "test_summary");
    Assert.assertEquals(issueInput.getField("description").getValue(), "test_description");
    Assert.assertEquals(issueInput.getField("test1").getValue().toString(), "ComplexIssueInputFieldValue{valuesMap={name=value1}}");
    Assert.assertEquals(issueInput.getField("test2").getValue().toString(), "ComplexIssueInputFieldValue{valuesMap={name=value2}}");

    // Assert if all the required fields are sets
    Assert.assertEquals(issueInput.getFields().size(), 9);
    Assert.assertTrue(issueInput.getFields().keySet().contains("anotherrequiredfield"));
    Assert.assertFalse(issueInput.getFields().keySet().contains("notrequiredfield"));
  }
}
