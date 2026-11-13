"use client"

import { useState } from "react"
import { Button } from "@/components/ui/button"
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible"
import { ChevronDown, ChevronRight, Copy, Check } from "lucide-react"

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  return (
    <Button
      variant="ghost"
      size="icon-xs"
      aria-label={copied ? "JSON copied" : "Copy JSON"}
      title={copied ? "Copied" : "Copy JSON"}
      onClick={() => {
        navigator.clipboard.writeText(text)
        setCopied(true)
        setTimeout(() => setCopied(false), 1500)
      }}
    >
      {copied ? <Check className="size-3" /> : <Copy className="size-3" />}
    </Button>
  )
}

export function JsonViewer({
  data,
  title,
  defaultOpen = false,
}: {
  data: unknown
  title?: string
  defaultOpen?: boolean
}) {
  const jsonStr = JSON.stringify(data, null, 2)
  const [open, setOpen] = useState(defaultOpen)

  if (title) {
    return (
      <Collapsible open={open} onOpenChange={setOpen}>
        <div className="flex items-center gap-2">
          <CollapsibleTrigger className="flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground">
            {open ? (
              <ChevronDown className="size-3.5" />
            ) : (
              <ChevronRight className="size-3.5" />
            )}
            {title}
          </CollapsibleTrigger>
          <CopyButton text={jsonStr} />
        </div>
        <CollapsibleContent>
          <pre className="mt-2 overflow-x-auto rounded-md bg-muted/50 p-3 font-mono text-xs leading-relaxed">
            {jsonStr}
          </pre>
        </CollapsibleContent>
      </Collapsible>
    )
  }

  return (
    <div className="relative">
      <div className="absolute top-1 right-1">
        <CopyButton text={jsonStr} />
      </div>
      <pre className="overflow-x-auto rounded-md bg-muted/50 p-3 font-mono text-xs leading-relaxed">
        {jsonStr}
      </pre>
    </div>
  )
}
