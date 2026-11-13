import type { Metadata } from "next"
import "./globals.css"

export const metadata: Metadata = {
  title: "Tenuto Console",
  description: "Tenuto workflow orchestration and debugging console",
}

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="h-full bg-background text-foreground">{children}</body>
    </html>
  )
}
