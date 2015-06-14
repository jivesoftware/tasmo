/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.jivesoftware.os.tasmo.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Sets;
import com.jivesoftware.os.tasmo.configuration.ViewObject.ViewArray;
import java.io.IOException;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNotNull;

/**
 *
 * @author pete
 */
public class ViewModelParserTest {

    String exampleViewNode = "{\"Test\":{"
        + "\"objectId\":\"Group_aaaaaaaaaacts|Space_aaaaaaaaaacts|Project_aaaaaaaaaacts\","
        + "\"owner\":{"
        + "\"objectId\":\"User_aaaaaaaaaacts|ExternalUser_aaaaaaaaaacts|AnonymousUser_aaaaaaaaaacts\","
        + "\"firstName\":\"unset\","
        + "\"lastName\":\"unset\""
        + "},"
        + "\"all_parent\":[{"
        + "\"objectId\":\"Document_aaaaaaaaaacts|Thread_aaaaaaaaaacts|BlogPost_aaaaaaaaaacts\","
        + "\"subject\":\"unset\","
        + "\"body\":\"unset\""
        + "}]"
        + "}}";

    @Test
    public void testBuildViewFromMultiFieldExample() throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        ViewModel.ViewConfigurationBuilder builder = ViewModel.builder(mapper.readValue(exampleViewNode, ObjectNode.class));
        ViewModel model = builder.build();

        assertEquals(model.getViewClassName(), "Test");
        ViewObject object = model.getViewObject();
        Set<String> classNames = Sets.newHashSet("Group", "Space", "Project");
        assertTrue(Sets.difference(classNames, object.getTriggeringEventClassNames()).isEmpty());


        ViewObject ownerObject = object.getRefFields().get("owner");
        assertNotNull(ownerObject);
        classNames = Sets.newHashSet("User", "ExternalUser", "AnonymousUser");
        assertTrue(Sets.difference(classNames, ownerObject.getTriggeringEventClassNames()).isEmpty());

        ViewArray contentObject = object.getBackRefFields().get("parent");
        assertNotNull(contentObject);

        classNames = Sets.newHashSet("Document", "Thread", "BlogPost");
        assertTrue(Sets.difference(classNames, contentObject.element.getTriggeringEventClassNames()).isEmpty());

    }
}
