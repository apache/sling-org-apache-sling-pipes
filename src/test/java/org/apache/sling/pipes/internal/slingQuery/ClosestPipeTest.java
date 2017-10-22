package org.apache.sling.pipes.internal.slingQuery;

import org.apache.sling.pipes.AbstractPipeTest;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class ClosestPipeTest extends AbstractPipeTest {

    @Test
    public void testClosest() throws Exception {
        Set<String> outputs = plumber.newPipe(context.resourceResolver())
                .echo(SAME_COLOR)
                .closest("[color=green]").run();
        assertEquals("there should be 1 output", 1, outputs.size());
        assertTrue("there should be pea", outputs.contains(PATH_PEA));
    }
}