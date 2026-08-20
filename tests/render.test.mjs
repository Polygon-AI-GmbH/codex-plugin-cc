import test from "node:test";
import assert from "node:assert/strict";

import { renderReviewResult, renderStoredJobResult } from "../plugins/codex/scripts/lib/render.mjs";

test("renderReviewResult degrades gracefully when JSON is missing required review fields", () => {
  const output = renderReviewResult(
    {
      parsed: {
        verdict: "approve",
        summary: "Looks fine."
      },
      rawOutput: JSON.stringify({
        verdict: "approve",
        summary: "Looks fine."
      }),
      parseError: null
    },
    {
      reviewLabel: "Adversarial Review",
      targetLabel: "working tree diff"
    }
  );

  assert.match(output, /Codex returned JSON with an unexpected review shape\./);
  assert.match(output, /Missing array `findings`\./);
  assert.match(output, /Raw final message:/);
});

test("renderStoredJobResult prefers rendered output for structured review jobs", () => {
  const output = renderStoredJobResult(
    {
      id: "review-123",
      status: "completed",
      title: "Codex Adversarial Review",
      jobClass: "review",
      threadId: "thr_123"
    },
    {
      threadId: "thr_123",
      rendered: "# Codex Adversarial Review\n\nTarget: working tree diff\nVerdict: needs-attention\n",
      result: {
        result: {
          verdict: "needs-attention",
          summary: "One issue.",
          findings: [],
          next_steps: []
        },
        rawOutput:
          '{"verdict":"needs-attention","summary":"One issue.","findings":[],"next_steps":[]}'
      }
    }
  );

  assert.match(output, /^# Codex Adversarial Review/);
  assert.doesNotMatch(output, /^\{/);
  assert.match(output, /Codex session ID: thr_123/);
  assert.match(output, /Resume in Codex: codex resume thr_123/);
});

test("renderStoredJobResult keeps the review header for native review jobs", () => {
  const output = renderStoredJobResult(
    {
      id: "review-456",
      status: "completed",
      title: "Codex Review",
      jobClass: "review",
      threadId: "thr_456"
    },
    {
      threadId: "thr_456",
      rendered: "# Codex Review\n\nTarget: working tree diff\n\nNo blocking issues found.\n",
      result: {
        review: "Review",
        target: { label: "working tree diff" },
        threadId: "thr_456",
        codex: {
          status: 0,
          stderr: "",
          stdout: "No blocking issues found."
        }
      }
    }
  );

  assert.match(output, /^# Codex Review/);
  assert.match(output, /Target: working tree diff/);
  assert.match(output, /Codex session ID: thr_456/);
  assert.match(output, /Resume in Codex: codex resume thr_456/);
});

test("renderStoredJobResult rebuilds the native review header when rendered output is missing", () => {
  const output = renderStoredJobResult(
    {
      id: "review-789",
      status: "completed",
      title: "Codex Review",
      jobClass: "review"
    },
    {
      result: {
        review: "Review",
        target: { label: "branch diff against main" },
        threadId: null,
        codex: {
          status: 0,
          stderr: "",
          stdout: "One minor issue."
        }
      }
    }
  );

  assert.match(output, /^# Codex Review/);
  assert.match(output, /Target: branch diff against main/);
  assert.match(output, /One minor issue\./);
});
