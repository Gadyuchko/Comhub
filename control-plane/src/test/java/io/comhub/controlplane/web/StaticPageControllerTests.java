package io.comhub.controlplane.web;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.forwardedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(StaticPageController.class)
class StaticPageControllerTests {

    @Autowired
    MockMvc mockMvc;

    @Test
    void forwardsReactBrowserRoutesToIndexHtml() throws Exception {
        mockMvc.perform(get("/config"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));

        mockMvc.perform(get("/dlq"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));

        mockMvc.perform(get("/dashboards"))
                .andExpect(status().isOk())
                .andExpect(forwardedUrl("/index.html"));
    }
}
