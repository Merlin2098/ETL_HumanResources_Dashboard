# Instructions: Modular Agent Invocation Flow

This project uses a modular architecture where Agent behavior is defined by **Context**, **Workflow**, and **Simplified Prompts**. You can trigger agents using two methods: the **Chat-Only Flow** or the **Manifest-Driven Flow (Recommended)**.

---

## Skill Loading Architecture (v2.0.0)

Skills are loaded using a **three-layer lazy loading** model designed for portability across prompt-based runtimes (VS Code, Claude Code, Antigravity, etc.).

### Layer 1: Skill Index (Always Loaded)
**File:** `agent/skills/_index.yaml` (~5K tokens)

Injected at session start. Contains all 98 skills with: name, tier, cluster, priority, and one-line purpose. This is the **minimum viable context** for skill selection.

### Layer 2: Skill Headers (On-Demand)
**Files:** `agent/skills/<category>/<skill_name>.meta.yaml` (~200 tokens each)

Loaded when a cluster is activated or a skill is being evaluated. Contains structured metadata: triggers, dependencies, exclusive groups, agent bindings.

### Layer 3: Full Skill Bodies (On-Demand)
**Files:** `agent/skills/<category>/<skill_name>.md` (~500-2000 tokens each)

Loaded only when a skill is actively invoked. Contains the full implementation instructions.

### Skill Tiers

| Tier | Count | Loading Behavior |
| :--- | :---: | :--- |
| **Core** | 26 | Layer 1 always loaded. Layer 3 loaded at session start for governance skills. |
| **Lazy** | 54 | Layer 1 always loaded. Layers 2-3 loaded on-demand via triggers. |
| **Dormant** | 18 | Layer 1 only. No implementation files active. Registry placeholder. |

### Deterministic Trigger Engine
**File:** `agent/skills/_trigger_engine.yaml`

External orchestrators can evaluate deterministic rules (file extensions, pipeline phases, error patterns) **without an LLM** to narrow skill candidates before any prompt is sent. After deterministic filtering, remaining ambiguity is resolved by the LLM.

---

## Method 1: Manifest-Driven Flow (Recommended)

This method allows you to define your task in your IDE and trigger it with a short command.

### 1. Prepare the Manifest
Open `agent/user_task.yaml` in your editor and fill in the task details:

```yaml
role: senior  # Who should do the work?
mode: ANALYZE_AND_IMPLEMENT
objective: "Your detailed task description here."
files: ["list/your/files.py"]
constraints: "Any restrictions."
skill_hints: ["skill_name_1", "skill_name_2"]  # Optional: suggest specific skills
```

### 2. Trigger the Agent
In the chat with the agent, use the **Simplified Prompt** (provided in `agent/<agent_role>/prompt/simplified_prompt.md`) but leave the placeholders empty, or simply say:

> **"Run task from agent/user_task.yaml"**

### 3. Agent Response
The agent will:
1. Load the Skill Index (`_index.yaml`) if not already in context.
2. Load its defined Context and Workflow from its internal memory.
3. Read the `user_task.yaml` file.
4. Evaluate skill hints and deterministic triggers to narrow candidates.
5. Load relevant skill headers (Layer 2) and bodies (Layer 3) as needed.
6. Automatically begin Step 1 of its Workflow.

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

## Skill System References

| File | Purpose |
| :--- | :--- |
| `agent/skills/skills_registry.yaml` | Authoritative skill registry (v2.0.0) — full metadata |
| `agent/skills/_index.yaml` | Compact skill index for prompt injection (~5K tokens) |
| `agent/skills/_trigger_engine.yaml` | Deterministic trigger rules for external orchestrators |
| `agent/skills/<category>/<skill>.meta.yaml` | Layer 2 skill headers (~200 tokens each) |
| `agent/skills/<category>/<skill>.md` | Layer 3 full skill bodies |

**Note:** All agents use `agent/agent_outputs/context.json` as their single authoritative source of project knowledge (Skills, Rules, Treemap).
