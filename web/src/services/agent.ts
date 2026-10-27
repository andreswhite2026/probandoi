import { apiFetch } from './api'

export interface AgentQueryResponse {
  answer: string
  tool_used: string
  data: any
}

export async function askAgent(question: string): Promise<AgentQueryResponse> {
  return apiFetch<AgentQueryResponse>('/api/v1/agent/query', {
    method: 'POST',
    body: JSON.stringify({ question })
  })
}
