import { Intent, IToaster, IToastProps, Position, Toaster } from '@blueprintjs/core'
import type { editor as EDITOR } from 'monaco-editor/esm/vs/editor/editor.api'
import { get } from 'lodash-es'
import moment from 'moment'
import langMap from 'lang-map'

export const LIST_FETCHING_PER_PAGE = 20
export const BRANCH_PER_PAGE = 100
export const DEFAULT_DATE_FORMAT = 'MM/DD/YYYY hh:mm a'
export const DEFAULT_BRANCH_NAME = 'main'
export const REGEX_VALID_REPO_NAME = /^[a-zA-Z_][0-9a-zA-Z-_.$]*$/
export const SUGGESTED_BRANCH_NAMES = [DEFAULT_BRANCH_NAME, 'master']
export const FILE_SEPERATOR = '/'

/** This utility shows a toaster without being bound to any component.
 * It's useful to show cross-page/component messages */
export function showToaster(message: string, props?: Partial<IToastProps>): IToaster {
  const toaster = Toaster.create({ position: Position.TOP })
  toaster.show({ message, intent: Intent.SUCCESS, ...props })
  return toaster
}

export const getErrorMessage = (error: Unknown): string =>
  get(error, 'data.error', get(error, 'data.message', error?.message))

export interface SourceCodeEditorProps {
  source: string
  language?: string
  lineNumbers?: boolean
  readOnly?: boolean
  highlightLines?: string // i.e: {1,3-4}, TODO: not yet supported
  className?: string
  height?: number | string
  autoHeight?: boolean
  wordWrap?: boolean
  onChange?: (value: string) => void
}

// Monaco editor has a bug where when its value is set, the value
// is selected all by default.
// Fix by set selection range to zero
export const deselectAllMonacoEditor = (editor?: EDITOR.IStandaloneCodeEditor) => {
  editor?.focus()
  setTimeout(() => {
    editor?.setSelection(new monaco.Selection(0, 0, 0, 0))
  }, 0)
}

export const displayDateTime = (value: number): string | null => {
  return value ? moment.unix(value / 1000).format(DEFAULT_DATE_FORMAT) : null
}

const LOCALE = Intl.NumberFormat().resolvedOptions?.().locale || 'en-US'

/**
 * Format a timestamp to short format time (i.e: 7:41 AM)
 * @param timestamp Timestamp
 * @param timeStyle Optional DateTimeFormat's `timeStyle` option.
 */
export function formatTime(timestamp: number | string, timeStyle = 'short'): string {
  return new Intl.DateTimeFormat(LOCALE, {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore: TS built-in type for DateTimeFormat is not correct
    timeStyle
  }).format(new Date(timestamp))
}

/**
 * Format a timestamp to medium format date (i.e: Jan 1, 2021)
 * @param timestamp Timestamp
 * @param dateStyle Optional DateTimeFormat's `dateStyle` option.
 */
export function formatDate(timestamp: number | string, dateStyle = 'medium'): string {
  return new Intl.DateTimeFormat(LOCALE, {
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore: TS built-in type for DateTimeFormat is not correct
    dateStyle
  }).format(new Date(timestamp))
}

/**
 * Make any HTML element as a clickable button with keyboard accessibility
 * support (hit Enter/Space will trigger click event)
 */
export const ButtonRoleProps = {
  onKeyDown: (e: React.KeyboardEvent<HTMLDivElement>) => {
    if (e.key === 'Enter' || e.key === ' ' || e.key === 'Spacebar' || e.which === 13 || e.which === 32) {
      ;(e.target as unknown as { click: () => void })?.click?.()
    }
  },
  tabIndex: 0,
  role: 'button'
}

const MONACO_SUPPORTED_LANGUAGES = [
  'abap',
  'apex',
  'azcli',
  'bat',
  'cameligo',
  'clojure',
  'coffee',
  'cpp',
  'csharp',
  'csp',
  'css',
  'dockerfile',
  'fsharp',
  'go',
  'graphql',
  'handlebars',
  'html',
  'ini',
  'java',
  'javascript',
  'json',
  'kotlin',
  'less',
  'lua',
  'markdown',
  'mips',
  'msdax',
  'mysql',
  'objective-c',
  'pascal',
  'pascaligo',
  'perl',
  'pgsql',
  'php',
  'postiats',
  'powerquery',
  'powershell',
  'pug',
  'python',
  'r',
  'razor',
  'redis',
  'redshift',
  'restructuredtext',
  'ruby',
  'rust',
  'sb',
  'scheme',
  'scss',
  'shell',
  'solidity',
  'sophia',
  'sql',
  'st',
  'swift',
  'tcl',
  'twig',
  'typescript',
  'vb',
  'xml',
  'yaml'
]

const EXTENSION_TO_LANG: Record<string, string> = {
  tsx: 'typescript',
  jsx: 'typescript'
}

export const PLAIN_TEXT = 'plaintext'

export const filenameToLanguage = (name?: string): string | undefined => {
  const extension = name?.split('.').pop() || ''
  const map = langMap.languages(extension)

  if (map?.length) {
    return MONACO_SUPPORTED_LANGUAGES.find(lang => map.includes(lang)) || EXTENSION_TO_LANG[extension] || PLAIN_TEXT
  }

  return PLAIN_TEXT
}