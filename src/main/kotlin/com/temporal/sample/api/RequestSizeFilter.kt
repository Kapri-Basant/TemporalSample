package com.temporal.sample.api

import jakarta.servlet.FilterChain
import jakarta.servlet.ReadListener
import jakarta.servlet.ServletInputStream
import jakarta.servlet.http.HttpServletRequest
import jakarta.servlet.http.HttpServletRequestWrapper
import jakarta.servlet.http.HttpServletResponse
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.stereotype.Component
import org.springframework.web.filter.OncePerRequestFilter
import java.io.BufferedReader
import java.io.StringReader

@Component
@Order(Ordered.LOWEST_PRECEDENCE - 10)
class RequestSizeFilter : OncePerRequestFilter() {
    // Read at most 64 KiB before JSON parsing, including requests without Content-Length.
    override fun doFilterInternal(request: HttpServletRequest, response: HttpServletResponse, chain: FilterChain) {
        response.setHeader("Cache-Control", "no-store")
        if (!request.requestURI.startsWith("/api/") || request.method !in setOf("POST", "PUT", "PATCH")) {
            chain.doFilter(request, response)
            return
        }
        val limit = 65_536
        if (request.contentLengthLong > limit) {
            tooLarge(response)
            return
        }
        val bytes = request.inputStream.readNBytes(limit + 1)
        if (bytes.size > limit) {
            tooLarge(response)
            return
        }
        val body = object : HttpServletRequestWrapper(request) {
            // Replay the already bounded JSON bytes for the normal request converter.
            override fun getInputStream(): ServletInputStream = object : ServletInputStream() {
                private val input = bytes.inputStream()
                // Read one checked byte from the buffered request.
                override fun read(): Int = input.read()
                // Tell the synchronous converter when all saved bytes were read.
                override fun isFinished(): Boolean = input.available() == 0
                // Reading this small memory buffer never blocks on the network.
                override fun isReady(): Boolean = true
                // This API uses blocking servlet requests, not async body listeners.
                override fun setReadListener(listener: ReadListener?) = throw UnsupportedOperationException("Blocking requests only")
            }
            // Honor JSON text readers without re-reading the untrusted network stream.
            override fun getReader(): BufferedReader = BufferedReader(StringReader(bytes.toString(Charsets.UTF_8)))
        }
        chain.doFilter(body, response)
    }

    // Return a small fixed reply without echoing any rejected input.
    private fun tooLarge(response: HttpServletResponse) {
        response.status = 413
        response.contentType = "application/problem+json"
        response.writer.write("""{"title":"Request too large","status":413,"detail":"JSON requests must fit in 64 KiB"}""")
    }
}
