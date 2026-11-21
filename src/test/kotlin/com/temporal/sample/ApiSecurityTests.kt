package com.temporal.sample

import org.junit.jupiter.api.Test
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.mock.web.MockServletContext
import org.springframework.security.config.annotation.web.builders.HttpSecurity
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.jwt
import org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity
import org.springframework.security.web.SecurityFilterChain
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post
import org.springframework.test.web.servlet.result.MockMvcResultMatchers.status
import org.springframework.test.web.servlet.setup.DefaultMockMvcBuilder
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext
import org.springframework.web.servlet.config.annotation.EnableWebMvc

class ApiSecurityTests {
    // Check real filters so anonymous users and wrong scopes cannot reach the API.
    @Test
    fun `production routes enforce scopes and hide demo controls`() {
        val context = AnnotationConfigWebApplicationContext()
        context.servletContext = MockServletContext()
        context.register(SecurityTestApp::class.java)
        context.refresh()
        try {
            val mvc = MockMvcBuilders.webAppContextSetup(context)
                .apply<DefaultMockMvcBuilder>(springSecurity()).build()
            mvc.perform(get("/actuator/health/readiness")).andExpect(status().isOk)
            mvc.perform(get("/api/v1/jobs")).andExpect(status().isUnauthorized)
            mvc.perform(get("/api/v1/jobs").with(jwt())).andExpect(status().isForbidden)
            mvc.perform(get("/api/v1/jobs").with(jwt().authorities(SimpleGrantedAuthority("SCOPE_workflow.read"))))
                .andExpect(status().isOk)
            mvc.perform(post("/api/v1/jobs").with(jwt().authorities(SimpleGrantedAuthority("SCOPE_workflow.read"))))
                .andExpect(status().isForbidden)
            mvc.perform(post("/api/v1/jobs").with(jwt().authorities(SimpleGrantedAuthority("SCOPE_workflow.write"))))
                .andExpect(status().isOk)
            mvc.perform(post("/demo/admin/crash-worker").with(jwt().authorities(SimpleGrantedAuthority("SCOPE_workflow.write"))))
                .andExpect(status().isForbidden)
        } finally {
            context.close()
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnableWebSecurity
    @EnableWebMvc
    class SecurityTestApp {
        // Use the production filter rules with a small test application.
        @Bean
        fun filterChain(http: HttpSecurity): SecurityFilterChain = SecurityConfig().productionSecurity(http)

        // Tests add authenticated callers directly; raw bearer tokens remain rejected.
        @Bean
        fun decoder(): JwtDecoder = JwtDecoder { throw JwtException("No test signature is trusted") }

        // Supply simple routes so the tests measure authorization rather than controllers.
        @Bean
        fun routes(): SecurityTestRoutes = SecurityTestRoutes()
    }

    @RestController
    class SecurityTestRoutes {
        // Return one API response when the caller has read access.
        @GetMapping("/api/v1/jobs")
        fun read(): String = "ok"

        // Return one API response when the caller has write access.
        @PostMapping("/api/v1/jobs")
        fun write(): String = "ok"

        // Keep readiness available for Kubernetes without revealing details.
        @GetMapping("/actuator/health/readiness")
        fun health(): String = "UP"
    }
}
