# Instructions: Modular Agent Invocation Flow

This project uses a modular architecture where Agent behavior is defined by **Context**, **Workflow**, and **Simplified Prompts**. You can trigger agents using two methods: the **Chat-Only Flow** or the **Manifest-Driven Flow (Recommended)**.

---

## Method 1: Manifest-Driven Flow (Efficiency Meta)

This method allows you to define your task in your IDE and trigger it with a short command.

### 1. Prepare the Manifest
Open `agent/user_task.yaml` in your editor and fill in the task details:

```yaml
role: senior  # Who should do the work?
mode: ANALYZE_AND_IMPLEMENT
objective: "Your detailed task description here."
files: ["list/your/files.py"]
constraints: "Any restrictions."
```

### 2. Trigger the Agent
In the chat with the agent, use the **Simplified Prompt** (provided in `agent/<agent_role>/prompt/simplified_prompt.md`) but leave the placeholders empty, or simply say:

> **"Run task from agent/user_task.yaml"**

### 3. Agent Response
The agent will:
1. Load its defined Context and Workflow from its internal memory.
2. Read the `user_task.yaml` file.
3. Automatically begin Step 1 of its Workflow.

---

## Method 2: Chat-Only Flow (Manual)

Use this for quick tasks where you don't want to create a file.

### 1. Copy the Prompt Template
Navigate to the desired agent's prompt directory:
- Senior: `agent/agent_senior/prompt/simplified_prompt.md`
- Executor: `agent/agent_executor/prompt/simplified_prompt.md`
- Inspector: `agent/agent_inspector/prompt/simplified_prompt.md`

### 2. Fill and Send
Copy the content, replace the `{PLACEHOLDERS}` (Objective, Files, etc.) with your specific task, and paste it into the chat.

---

## Critical References

| Agent | Context (The Rules) | Workflow (The Steps) |
| :--- | :--- | :--- |
| **Senior** | `agent/agent_senior/context/context.md` | `agent/agent_senior/workflow/workflow.md` |
| **Executor** | `agent/agent_executor/context/context.md` | `agent/agent_executor/workflow/workflow.md` |
| **Inspector** | `agent/agent_inspector/context/context.md` | `agent/agent_inspector/workflow/workflow.md` |

**Note:** All agents use `agent/agent_outputs/context.json` as their single authoritative source of project knowledge (Skills, Rules, Treemap).
