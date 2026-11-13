<script setup lang="ts">
import { computed } from "vue"

type FeatureStatus = "Stable" | "Preview" | "Internal"

const props = defineProps<{
  status: FeatureStatus
  since?: string
  note?: string
}>()

const statusText = computed(() => ({
  Stable: "稳定能力",
  Preview: "预览能力",
  Internal: "内部接口",
})[props.status])
</script>

<template>
  <div class="feature-status" :class="`feature-status--${status.toLowerCase()}`">
    <span class="feature-status__badge">{{ status }}</span>
    <span class="feature-status__label">{{ statusText }}</span>
    <span v-if="since" class="feature-status__since">自 {{ since }}</span>
    <span v-if="note" class="feature-status__note">{{ note }}</span>
  </div>
</template>
