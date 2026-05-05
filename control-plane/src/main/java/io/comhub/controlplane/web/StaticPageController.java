package io.comhub.controlplane.web;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Serves browser routes owned by the React application.
 *
 * @author Roman Hadiuchko
 */
@Controller
public class StaticPageController {

    @GetMapping({"/config", "/dlq", "/dashboards"})
    public String forwardBrowserRoute() {
        return "forward:/index.html";
    }
}
