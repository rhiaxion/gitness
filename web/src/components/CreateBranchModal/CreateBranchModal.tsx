import React, { useCallback, useState } from 'react'
import { Dialog, Intent } from '@blueprintjs/core'
import * as yup from 'yup'
import {
  Button,
  ButtonProps,
  Container,
  Layout,
  FlexExpander,
  Icon,
  Formik,
  FormikForm,
  Heading,
  useToaster,
  FormInput,
  Label,
  ButtonVariation
} from '@harness/uicore'
import { FontVariation } from '@harness/design-system'
import { useMutate } from 'restful-react'
import { get } from 'lodash-es'
import { useModalHook } from '@harness/use-modal'
import { useStrings } from 'framework/strings'
import { getErrorMessage, permissionProps } from 'utils/Utils'
import { CodeIcon, GitInfoProps, isGitBranchNameValid } from 'utils/GitUtils'
import { BranchTagSelect } from 'components/BranchTagSelect/BranchTagSelect'
import type { RepoBranch } from 'services/code'
import { useGetSpaceParam } from 'hooks/useGetSpaceParam'
import { useAppContext } from 'AppContext'
import css from './CreateBranchModal.module.scss'

interface FormData {
  name: string
  sourceBranch: string
}

interface UseCreateBranchModalProps extends Pick<GitInfoProps, 'repoMetadata'> {
  suggestedBranchName?: string
  suggestedSourceBranch?: string
  onSuccess: (data: RepoBranch) => void
  showSuccessMessage?: boolean
  showBranchTag?: boolean
  refIsATag?: boolean
}

interface CreateBranchModalButtonProps extends Omit<ButtonProps, 'onClick'>, UseCreateBranchModalProps {
  onSuccess: (data: RepoBranch) => void
  showSuccessMessage?: boolean
}

export function useCreateBranchModal({
  suggestedBranchName = '',
  suggestedSourceBranch = '',
  onSuccess,
  repoMetadata,
  showSuccessMessage,
  showBranchTag = true,
  refIsATag = false
}: UseCreateBranchModalProps) {
  const [branchName, setBranchName] = useState(suggestedBranchName)
  const ModalComponent: React.FC = () => {
    const { getString } = useStrings()
    const [sourceBranch, setSourceBranch] = useState(suggestedSourceBranch || (repoMetadata.default_branch as string))
    const { showError, showSuccess } = useToaster()
    const { mutate: createBranch, loading } = useMutate<RepoBranch>({
      verb: 'POST',
      path: `/api/v1/repos/${repoMetadata.path}/+/branches`
    })
    const handleSubmit = (formData: FormData) => {
      const name = get(formData, 'name').trim()
      try {
        createBranch({
          name,
          target: sourceBranch
        })
          .then(response => {
            hideModal()
            onSuccess(response)
            if (showSuccessMessage) {
              showSuccess(getString('branchCreated', { branch: name }), 5000)
            }
          })
          .catch(_error => {
            showError(getErrorMessage(_error), 0, 'failedToCreateBranch')
          })
      } catch (exception) {
        showError(getErrorMessage(exception), 0, 'failedToCreateBranch')
      }
    }

    return (
      <Dialog
        isOpen
        enforceFocus={false}
        onClose={hideModal}
        title={''}
        style={{ width: 585, maxHeight: '95vh', overflow: 'auto' }}>
        <Layout.Vertical style={{ height: '100%' }} className={css.main}>
          <Heading className={css.title} font={{ variation: FontVariation.H3 }} margin={{ bottom: 'xlarge' }}>
            {getString('createABranch')}
          </Heading>
          <Container className={css.container}>
            <Formik<FormData>
              initialValues={{
                name: branchName,
                sourceBranch: suggestedSourceBranch
              }}
              formName="createGitBranch"
              enableReinitialize={true}
              validationSchema={yup.object().shape({
                name: yup
                  .string()
                  .trim()
                  .required()
                  .test('valid-branch-name', getString('validation.gitBranchNameInvalid'), value => {
                    const val = value || ''
                    return !!val && isGitBranchNameValid(val)
                  })
              })}
              validateOnChange
              validateOnBlur
              onSubmit={handleSubmit}>
              <FormikForm>
                <FormInput.Text
                  name="name"
                  label={getString('name')}
                  placeholder={getString('enterBranchPlaceholder')}
                  tooltipProps={{
                    dataTooltipId: 'repositoryBranchTextField'
                  }}
                  inputGroup={{ autoFocus: true }}
                />
                <Container margin={{ top: 'medium' }}>
                  <Label className={css.label}>{getString('basedOn')}</Label>
                  {/* <Text className={css.branchSourceDesc}>{getString('branchSourceDesc')}</Text> */}
                  <Layout.Horizontal className={css.selectContainer} padding={{ top: 'xsmall' }}>
                    <BranchTagSelect
                      className={css.branchTagSelect}
                      repoMetadata={repoMetadata}
                      disableBranchCreation
                      disableViewAllBranches
                      forBranchesOnly={showBranchTag}
                      gitRef={refIsATag ? `refs/tags/${sourceBranch}` : sourceBranch}
                      onSelect={setSourceBranch}
                      popoverClassname={css.popoverContainer}
                    />
                  </Layout.Horizontal>
                </Container>

                <Layout.Horizontal
                  spacing="small"
                  padding={{ right: 'xxlarge', top: 'xxlarge', bottom: 'large' }}
                  style={{ alignItems: 'center' }}>
                  <Button
                    type="submit"
                    text={getString('createBranch')}
                    variation={ButtonVariation.PRIMARY}
                    disabled={loading}
                  />
                  <Button text={getString('cancel')} variation={ButtonVariation.LINK} onClick={hideModal} />
                  <FlexExpander />

                  {loading && <Icon intent={Intent.PRIMARY} name="spinner" size={16} />}
                </Layout.Horizontal>
              </FormikForm>
            </Formik>
          </Container>
        </Layout.Vertical>
      </Dialog>
    )
  }
  const [openModal, hideModal] = useModalHook(ModalComponent, [
    onSuccess,
    suggestedBranchName,
    suggestedSourceBranch,
    showSuccessMessage
  ])
  const fn = useCallback(
    (_branchName?: string) => {
      if (_branchName) {
        setBranchName(_branchName)
      }
      openModal()
    },
    [setBranchName, openModal]
  )

  return fn
}

export const CreateBranchModalButton: React.FC<CreateBranchModalButtonProps> = ({
  onSuccess,
  repoMetadata,
  showSuccessMessage,
  ...props
}) => {
  const openModal = useCreateBranchModal({ repoMetadata, onSuccess, showSuccessMessage, showBranchTag: false })
  const { standalone } = useAppContext()
  const { hooks } = useAppContext()
  const space = useGetSpaceParam()

  const permPushResult = hooks?.usePermissionTranslate?.(
    {
      resource: {
        resourceType: 'CODE_REPOSITORY'
      },
      permissions: ['code_repo_push']
    },
    [space]
  )
  return <Button onClick={() => openModal()} {...props} {...permissionProps(permPushResult, standalone)} />
}
